import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import io
import time
from functools import wraps

# --- CẤU HÌNH ---
ADMIN_PASSWORD = st.secrets["admin_password"]
CACHE_TTL = 300  # Cache 5 phút (300 giây)

ALL_COLUMNS = [
    'STT', 'ID', 'Họ và tên *', 'Tên gọi khác', 'Giới tính *', 
    'Sinh ngày * (dd/mm/yyyy)', 'Dân tộc *', 'Tôn giáo *', 
    'Số định danh cá nhân *', 'Số thẻ Đảng* (12 số theo HD38-HD/BTCTW)',
    'Nơi cấp thẻ Đảng', 'Ngày cấp thẻ Đảng (dd/mm/yyyy)', 
    'Số thẻ theo Đảng quyết định 85',
    'Tổ chức Đảng đang sinh hoạt * (không sửa)', 
    'Nơi đăng ký khai sinh - Quốc gia *',
    'Nơi đăng ký khai sinh - Tỉnh *', 
    'Nơi đăng ký khai sinh - Địa chỉ chi tiết *',
    'Quê quán (theo mô hình 2 cấp) - Quốc gia *', 
    'Quê quán (theo mô hình 2 cấp) - Tỉnh *',
    'Quê quán (theo mô hình 2 cấp) - Địa chỉ chi tiết *', 
    'Thường trú (theo mô hình 2 cấp) - Quốc gia *',
    'Thường trú (theo mô hình 2 cấp) - Tỉnh *', 
    'Thường trú (theo mô hình 2 cấp) - Địa chỉ chi tiết *',
    'Ngày vào Đảng* (dd/mm/yyyy)', 
    'Ngày vào Đảng chính thức* (dd/mm/yyyy)', 
    'Số CMND cũ (nếu có)',
    'Trạng thái hoạt động', 
    'Ngày rời khỏi/ Ngày mất/ Ngày miễn sinh hoạt Đảng (dd/mm/yyyy)',
    'Đề nghị xóa (do đảng viên không thuộc chi bộ)/ (Nếu muốn xóa chọn "có", còn không bỏ qua)',
    'Temp_XaPhuong_KhaiSinh', 'Temp_ThonTo_KhaiSinh', 
    'Temp_XaPhuong_ThuongTru', 'Temp_ThonTo_ThuongTru'
]

TEMP_COLS = ['Temp_XaPhuong_KhaiSinh', 'Temp_ThonTo_KhaiSinh', 
             'Temp_XaPhuong_ThuongTru', 'Temp_ThonTo_ThuongTru']

READ_ONLY_COLS = [
    'STT', 'ID', 'Họ và tên *', 'Sinh ngày * (dd/mm/yyyy)', 
    'Tổ chức Đảng đang sinh hoạt * (không sửa)',
    'Đề nghị xóa (do đảng viên không thuộc chi bộ)/ (Nếu muốn xóa chọn "có", còn không bỏ qua)'
]

SHEET_NAME_MAIN = "Sheet1"
SHEET_NAME_BACKUP = "Backup"

# ========================================
# 🔥 GIẢI PHÁP AUTO-RETRY KHI GẶP LỖI 429
# ========================================

def retry_on_rate_limit(max_retries=5, initial_wait=2):
    """
    Decorator tự động retry khi gặp lỗi 429 (Rate Limit)
    
    Args:
        max_retries: Số lần thử lại tối đa (default: 5)
        initial_wait: Thời gian chờ ban đầu (giây, default: 2)
    
    Exponential backoff: 2s -> 4s -> 8s -> 16s -> 32s
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            wait_time = initial_wait
            
            for attempt in range(max_retries):
                try:
                    # Thử thực hiện hàm
                    return func(*args, **kwargs)
                
                except gspread.exceptions.APIError as e:
                    # Kiểm tra xem có phải lỗi 429 không
                    if e.response.status_code == 429:
                        if attempt < max_retries - 1:  # Còn lần thử
                            # Hiển thị thông báo thân thiện
                            with st.spinner(
                                f"⏳ Hệ thống đang bận, đang chờ {wait_time}s... "
                                f"(Lần thử {attempt + 1}/{max_retries})"
                            ):
                                time.sleep(wait_time)
                            
                            # Tăng thời gian chờ gấp đôi (exponential backoff)
                            wait_time *= 2
                        else:
                            # Hết lượt thử
                            st.error(
                                "❌ Hệ thống quá tải. Vui lòng thử lại sau 1 phút. "
                                "Nếu lỗi lặp lại, liên hệ admin."
                            )
                            raise
                    else:
                        # Lỗi khác (không phải 429)
                        raise
                
                except Exception as e:
                    # Lỗi không xác định
                    st.error(f"⚠️ Lỗi không xác định: {str(e)}")
                    raise
            
            # Không bao giờ tới đây (đã raise ở trên)
            return None
        
        return wrapper
    return decorator

# ========================================
# ✅ ÁP DỤNG RETRY CHO TẤT CẢ REQUESTS
# ========================================

@st.cache_resource
def connect_to_workbook():
    """Kết nối 1 lần duy nhất, tái sử dụng cho toàn bộ app"""
    scope = ["https://spreadsheets.google.com/feeds", 
             "https://www.googleapis.com/auth/drive"]
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            "service_account.json", scope
        )
    except:
        import json
        key_dict = json.loads(st.secrets["textkey"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    
    client = gspread.authorize(creds)
    return client.open("DanhSachDangVien")

@retry_on_rate_limit(max_retries=5, initial_wait=2)
def safe_get_all_records(sheet, expected_headers):
    """Wrapper có retry cho get_all_records"""
    return sheet.get_all_records(expected_headers=expected_headers)

@retry_on_rate_limit(max_retries=5, initial_wait=2)
def safe_update_sheet(sheet, cell_range, values, value_input_option='USER_ENTERED'):
    """Wrapper có retry cho update"""
    return sheet.update(cell_range, values, value_input_option=value_input_option)

@retry_on_rate_limit(max_retries=5, initial_wait=2)
def safe_append_row(sheet, row_data, value_input_option='USER_ENTERED'):
    """Wrapper có retry cho append_row"""
    return sheet.append_row(row_data, value_input_option=value_input_option)

@retry_on_rate_limit(max_retries=5, initial_wait=2)
def safe_get_all_values(sheet):
    """Wrapper có retry cho get_all_values"""
    return sheet.get_all_values()

@st.cache_data(ttl=CACHE_TTL)
def load_data_main_cached(_sheet):
    """
    Load data 1 lần, cache 5 phút
    Có retry tự động khi gặp lỗi 429
    """
    # Sử dụng hàm safe thay vì gọi trực tiếp
    data = safe_get_all_records(_sheet, ALL_COLUMNS)
    df = pd.DataFrame(data)
    
    # Xử lý số 0 ở đầu
    cols_need_zero = [
        'Số định danh cá nhân *', 
        'Số thẻ Đảng* (12 số theo HD38-HD/BTCTW)',
        'Số CMND cũ (nếu có)'
    ]
    
    for col in cols_need_zero:
        if col in df.columns:
            df[col] = df[col].astype(str).replace(r'\.0$', '', regex=True)
            df[col] = df[col].replace(['nan', 'None', ''], '')
            df[col] = df[col].apply(
                lambda x: x.zfill(12) if x.strip() != '' and x.isdigit() else x
            )

    df['ID'] = df['ID'].astype(str).replace(r'\.0$', '', regex=True)
    return df

def load_data_main():
    """Wrapper để tương thích với code cũ"""
    workbook = connect_to_workbook()
    sheet = workbook.worksheet(SHEET_NAME_MAIN)
    df = load_data_main_cached(sheet)
    return df, sheet, workbook

# ========================================
# ✅ SESSION STATE MANAGEMENT
# ========================================

def init_session_data():
    """Khởi tạo data trong session_state khi cần"""
    if 'data_loaded' not in st.session_state:
        with st.spinner("🔄 Đang tải dữ liệu lần đầu..."):
            df, sheet, workbook = load_data_main()
            st.session_state.df_main = df
            st.session_state.main_sheet = sheet
            st.session_state.workbook = workbook
            st.session_state.data_loaded = True
            st.session_state.last_load_time = time.time()

def get_session_data():
    """Lấy data từ session thay vì load lại"""
    init_session_data()
    return (
        st.session_state.df_main,
        st.session_state.main_sheet,
        st.session_state.workbook
    )

def force_refresh_data():
    """
    Buộc refresh data - CHỈ DÀNH CHO ADMIN
    Xóa cache toàn cục để load data mới nhất
    """
    st.cache_data.clear()  # Xóa cache chung
    
    # Xóa session riêng của user hiện tại
    for key in ['data_loaded', 'df_main', 'main_sheet', 'workbook', 'last_load_time']:
        if key in st.session_state:
            del st.session_state[key]
    
    # Load lại data mới
    init_session_data()

# ========================================
# ✅ SAVE WITH RETRY
# ========================================

def save_update_optimized(sheet, row_index, updated_values, workbook):
    """
    Ghi 1 lần duy nhất với retry tự động
    ✅ SAU KHI LƯU → CHỈ XÓA SESSION CỦA USER HIỆN TẠI
    (Không xóa cache chung vì mỗi người chỉ sửa data của mình)
    """
    try:
        # 1. Chuẩn bị data
        row_vals = [updated_values.get(c, "") for c in ALL_COLUMNS]
        
        # 2. Backup (có retry)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            backup_sheet = workbook.worksheet(SHEET_NAME_BACKUP)
            safe_append_row(backup_sheet, [ts] + row_vals)
        except Exception as e:
            st.warning(f"⚠️ Không thể backup (không ảnh hưởng dữ liệu chính): {e}")
        
        # 3. Update Sheet chính (có retry)
        safe_update_sheet(sheet, f"A{row_index + 2}", [row_vals])
        
        # 4. Chỉ xóa session của user hiện tại để họ thấy data mới của mình
        # KHÔNG xóa cache chung vì không ảnh hưởng user khác
        for key in ['data_loaded', 'df_main', 'main_sheet', 'workbook']:
            if key in st.session_state:
                del st.session_state[key]
        
        return True
    
    except Exception as e:
        st.error(f"❌ Lỗi lưu dữ liệu: {str(e)}")
        return False

# ========================================
# ✅ ADMIN DASHBOARD WITH RETRY
# ========================================

@st.cache_data(ttl=CACHE_TTL)
def get_updated_ids(_backup_sheet):
    """Cache danh sách ID đã update với retry"""
    try:
        backup_rows = safe_get_all_values(_backup_sheet)
        if len(backup_rows) > 1:
            return set([
                str(row[2]).replace('.0', '') 
                for row in backup_rows[1:] 
                if len(row) > 2
            ])
        return set()
    except Exception as e:
        st.warning(f"⚠️ Không thể tải backup sheet: {e}")
        return set()

# ========================================
# 🎨 GIAO DIỆN CHÍNH
# ========================================

st.set_page_config(
    page_title="Cập nhật thông tin Đảng viên CBSV II -NEU", 
    layout="wide"
)

st.markdown("""
    <style>
    @media only screen and (max-width: 768px) {
        .block-container {
            padding-top: 4.5rem !important;
            padding-left: 1rem !important;
            padding-right: 1rem !important;
        }
        h1 { font-size: 1.6rem !important; }
        h2 { font-size: 1.3rem !important; }
        h3 { font-size: 1.1rem !important; }
    }
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR ---
st.sidebar.title("Menu")
app_mode = st.sidebar.radio(
    "Chọn chức năng:", 
    ["👤 Cập nhật thông tin", "📊 Admin Dashboard"]
)

# ✅ HIỂN THỊ TRẠNG THÁI CACHE (CHỈ CHO ADMIN)
if app_mode == "📊 Admin Dashboard":
    st.sidebar.divider()
    st.sidebar.markdown("### 📊 Trạng thái dữ liệu")

    if 'last_load_time' in st.session_state:
        elapsed = int(time.time() - st.session_state.last_load_time)
        minutes, seconds = divmod(elapsed, 60)
        
        if elapsed > 300:  # > 5 phút
            st.sidebar.warning(f"⚠️ Dữ liệu đã cũ {minutes}p {seconds}s")
        else:
            st.sidebar.success(f"✅ Cập nhật {minutes}p {seconds}s trước")
        
        if st.sidebar.button("🔄 Làm mới dữ liệu", help="Tải lại data mới nhất (dùng khi cần thống kê real-time)"):
            with st.spinner("Đang tải dữ liệu mới..."):
                force_refresh_data()
            st.rerun()
        
        st.sidebar.caption("💡 Cache tự động làm mới mỗi 5 phút")
    else:
        st.sidebar.info("Chưa tải dữ liệu")

# =========================================================
# CHẾ ĐỘ 1: NGƯỜI DÙNG CẬP NHẬT
# =========================================================

if app_mode == "👤 Cập nhật thông tin":
    st.title("📝 Cập nhật thông tin Đảng viên CBSV II -NEU")
    
    # Khởi tạo session states
    if 'step' not in st.session_state:
        st.session_state.step = 1
    if 'selected_row_index' not in st.session_state:
        st.session_state.selected_row_index = None

    # --- BƯỚC 1: TÌM KIẾM ---
    if st.session_state.step == 1:
        st.subheader("Bước 1: Tra cứu thông tin")
        
        if 'search_mode' not in st.session_state:
            st.session_state.search_mode = 'id'

        if st.session_state.search_mode == 'id':
            with st.form("search_id_form"):
                st.markdown("#### 🔍 Tra cứu bằng Số định danh cá nhân (CCCD/ĐDCN)")
                search_id = st.text_input(
                    "Nhập Số định danh cá nhân (12 số):", 
                    placeholder="Ví dụ: 030098123456"
                )
                submitted_id = st.form_submit_button("Tra cứu ngay", type="primary")

                if submitted_id:
                    if not search_id:
                        st.warning("Vui lòng nhập Số định danh cá nhân.")
                    else:
                        with st.spinner("🔍 Đang tìm kiếm..."):
                            df, _, _ = get_session_data()
                            
                            clean_input_id = search_id.strip()
                            mask = df['Số định danh cá nhân *'].astype(str).str.strip() == clean_input_id
                            results = df[mask]

                            if not results.empty:
                                st.success(f"✅ Tìm thấy: {results.iloc[0]['Họ và tên *']}")
                                st.session_state.search_results = results
                                st.session_state.step = 2
                                st.rerun()
                            else:
                                st.error(f"❌ Không tìm thấy: {clean_input_id}")
                                st.session_state.show_name_search_option = True

            if st.session_state.get('show_name_search_option', False):
                st.info("💡 Không tìm thấy? Thử tìm bằng Họ Tên.")
                if st.button("👉 Tìm bằng Họ Tên & Ngày Sinh"):
                    st.session_state.search_mode = 'name'
                    st.rerun()
            
            elif st.button("Chuyển sang tìm bằng Họ Tên & Ngày Sinh"):
                st.session_state.search_mode = 'name'
                st.rerun()

        elif st.session_state.search_mode == 'name':
            with st.form("search_name_form"):
                st.markdown("#### 👤 Tra cứu bằng Họ Tên và Ngày Sinh")
                col1, col2 = st.columns(2)
                with col1:
                    search_name = st.text_input("Họ và tên (đầy đủ có dấu):")
                with col2:
                    search_dob = st.text_input(
                        "Ngày sinh (dd/mm/yyyy):", 
                        placeholder="Ví dụ: 05/01/2005"
                    )
                
                submitted_name = st.form_submit_button("Tra cứu", type="primary")

                if submitted_name:
                    if not search_name or not search_dob:
                        st.warning("Vui lòng nhập đầy đủ.")
                    else:
                        with st.spinner("🔍 Đang tìm kiếm..."):
                            df, _, _ = get_session_data()
                            mask = (
                                df['Họ và tên *'].str.strip().str.lower() == search_name.strip().lower()
                            ) & (
                                df['Sinh ngày * (dd/mm/yyyy)'] == search_dob.strip()
                            )
                            results = df[mask]

                            if results.empty:
                                st.error("❌ Không tìm thấy.")
                                st.info("💡 Kiểm tra lại chính tả và định dạng ngày.")
                            else:
                                st.success(f"✅ Tìm thấy {len(results)} kết quả.")
                                st.session_state.search_results = results
                                st.session_state.step = 2
                                st.rerun()
            
            if st.button("⬅️ Quay lại tìm bằng Số định danh"):
                st.session_state.search_mode = 'id'
                st.session_state.show_name_search_option = False
                st.rerun()

    # --- BƯỚC 2: CHỌN NGƯỜI ---
    elif st.session_state.step == 2:
        st.subheader("Bước 2: Xác nhận danh tính")
        results = st.session_state.search_results
        
        st.info("Vui lòng chọn đúng tên của bạn trong danh sách dưới đây:")
        
        for index, row in results.iterrows():
            with st.container(border=True):
                c1, c2 = st.columns([4, 1])
                with c1:
                    st.markdown(f"**{row['Họ và tên *']}** - Sinh ngày: {row['Sinh ngày * (dd/mm/yyyy)']}")
                    st.text(f"Đơn vị: {row['Tổ chức Đảng đang sinh hoạt * (không sửa)']}")
                    st.text(f"Ngày vào Đảng: {row['Ngày vào Đảng* (dd/mm/yyyy)']}")
                with c2:
                    if st.button("CẬP NHẬT", key=f"btn_{index}", type="primary"):
                        st.session_state.selected_row_index = index
                        st.session_state.step = 3
                        st.rerun()
        
        st.write("---")
        if st.button("⬅️ Quay lại tìm kiếm"):
            st.session_state.step = 1
            st.rerun()

    # --- BƯỚC 3: CẬP NHẬT ---
    elif st.session_state.step == 3:
        st.subheader("Bước 3: Cập nhật thông tin chi tiết")
        
        # Load location data
        import json
        @st.cache_data
        def load_location_data():
            try:
                with open('vietnam_data.json', 'r', encoding='utf-8') as f:
                    return json.load(f)
            except FileNotFoundError:
                return {}

        vn_locations = load_location_data()
        list_tinh = list(vn_locations.keys())
        
        df, main_sheet, workbook = get_session_data()
        idx = st.session_state.selected_row_index
        
        try:
            current_data = df.loc[idx]
        except KeyError:
            st.error("⚠️ Phiên làm việc hết hạn. Vui lòng tìm kiếm lại.")
            st.stop()

        st.write("Kiểm tra và chỉnh sửa các thông tin dưới đây:")
        
        updated_values = {}

        # Danh sách Optional
        OPTIONAL_COLS = [
            'Số thẻ Đảng* (12 số theo HD38-HD/BTCTW)', 'Ngày cấp thẻ Đảng (dd/mm/yyyy)',
            'Số thẻ theo Đảng quyết định 85', 'Ngày vào Đảng chính thức* (dd/mm/yyyy)',
            'Nơi cấp thẻ Đảng', 'Số CMND cũ (nếu có)', 'Tên gọi khác'
        ]

        # --- FORM CẬP NHẬT (GIỮ NGUYÊN LOGIC CŨ) ---
        for col in ALL_COLUMNS:
            if col in TEMP_COLS:
                continue
            
            val = current_data.get(col, "")

            # KHAI SINH
            if col == 'Nơi đăng ký khai sinh - Quốc gia *':
                st.markdown("---")
                st.subheader("🏠 THÔNG TIN KHAI SINH")
                is_russia = str(val).strip().upper() in ["LIÊN BANG NGA", "NGA", "RUSSIA"]
                ks_quocgia = st.radio(
                    "Quốc gia *", 
                    ["Việt Nam", "Liên Bang Nga"], 
                    index=1 if is_russia else 0, 
                    horizontal=True, 
                    key="ks_qg"
                )
                updated_values[col] = ks_quocgia

            elif col == 'Nơi đăng ký khai sinh - Tỉnh *':
                cur_qg = st.session_state.get("ks_qg", "Việt Nam")
                if cur_qg == "Liên Bang Nga":
                    st.text_input("Tỉnh *", value="KHÔNG", disabled=True, key="ks_tinh_nga")
                    updated_values[col] = "KHÔNG"
                else:
                    try:
                        idx_t = list_tinh.index(str(val))
                    except:
                        idx_t = 0
                    ks_tinh = st.selectbox("Tỉnh *", list_tinh, index=idx_t, key="ks_tinh_vn")
                    updated_values[col] = ks_tinh

            elif col == 'Nơi đăng ký khai sinh - Địa chỉ chi tiết *':
                cur_qg = st.session_state.get("ks_qg", "Việt Nam")
                if cur_qg == "Liên Bang Nga":
                    c1, c2 = st.columns(2)
                    with c1:
                        st.text_input("Xã/Phường *", value="KHÔNG", disabled=True, key="ks_xa_nga")
                    with c2:
                        st.text_input("Địa chỉ chi tiết *", value="KHÔNG", disabled=True, key="ks_thon_nga")
                    updated_values['Temp_XaPhuong_KhaiSinh'] = "KHÔNG"
                    updated_values['Temp_ThonTo_KhaiSinh'] = "KHÔNG"
                    updated_values[col] = "KHÔNG"
                else:
                    cur_tinh = st.session_state.get("ks_tinh_vn", list_tinh[0] if list_tinh else "")
                    list_xa = vn_locations.get(cur_tinh, [])
                    
                    val_xa = current_data.get('Temp_XaPhuong_KhaiSinh', '')
                    val_thon = current_data.get('Temp_ThonTo_KhaiSinh', '')
                    if not val_xa and str(val):
                        parts = str(val).split(',')
                        if len(parts) >= 2:
                            val_xa = parts[-1].strip()
                            val_thon = ",".join(parts[:-1]).strip()

                    c1, c2 = st.columns(2)
                    with c1:
                        try:
                            idx_x = list_xa.index(val_xa)
                        except:
                            idx_x = 0
                        input_xa = st.selectbox("Xã/Phường *", list_xa, index=idx_x, key="ks_xa_vn")
                    with c2:
                        input_thon = st.text_input("Địa chỉ chi tiết *", value=str(val_thon), key="ks_thon_vn")
                    
                    updated_values['Temp_XaPhuong_KhaiSinh'] = input_xa
                    updated_values['Temp_ThonTo_KhaiSinh'] = input_thon
                    updated_values[col] = f"{input_thon}, {input_xa}".strip(", ")

            # QUÊ QUÁN
            elif col == 'Quê quán (theo mô hình 2 cấp) - Quốc gia *':
                st.markdown("---")
                st.subheader("🏠 THÔNG TIN QUÊ QUÁN")
                st.text_input("Quốc gia *", value="Việt Nam", disabled=True, key="qq_qg")
                updated_values[col] = "Việt Nam"

            elif col == 'Quê quán (theo mô hình 2 cấp) - Tỉnh *':
                try:
                    idx_t = list_tinh.index(str(val))
                except:
                    idx_t = 0
                qq_tinh = st.selectbox("Tỉnh *", list_tinh, index=idx_t, key="qq_tinh")
                updated_values[col] = qq_tinh

            elif col == 'Quê quán (theo mô hình 2 cấp) - Địa chỉ chi tiết *':
                cur_tinh = st.session_state.get("qq_tinh", "")
                list_xa = vn_locations.get(cur_tinh, [])
                try:
                    idx_x = list_xa.index(str(val))
                except:
                    idx_x = 0
                qq_xa = st.selectbox("Xã/Phường *", list_xa, index=idx_x, key="qq_xa")
                updated_values[col] = qq_xa

            # THƯỜNG TRÚ
            elif col == 'Thường trú (theo mô hình 2 cấp) - Quốc gia *':
                st.markdown("---")
                st.subheader("🏠 THÔNG TIN THƯỜNG TRÚ")
                st.text_input("Quốc gia *", value="Việt Nam", disabled=True, key="tt_qg")
                updated_values[col] = "Việt Nam"

            elif col == 'Thường trú (theo mô hình 2 cấp) - Tỉnh *':
                try:
                    idx_t = list_tinh.index(str(val))
                except:
                    idx_t = 0
                tt_tinh = st.selectbox("Tỉnh *", list_tinh, index=idx_t, key="tt_tinh")
                updated_values[col] = tt_tinh

            elif col == 'Thường trú (theo mô hình 2 cấp) - Địa chỉ chi tiết *':
                cur_tinh = st.session_state.get("tt_tinh", "")
                list_xa = vn_locations.get(cur_tinh, [])
                
                val_xa = current_data.get('Temp_XaPhuong_ThuongTru', '')
                val_thon = current_data.get('Temp_ThonTo_ThuongTru', '')
                if not val_xa and str(val):
                    parts = str(val).split(',')
                    if len(parts) >= 2:
                        val_xa = parts[-1].strip()
                        val_thon = ",".join(parts[:-1]).strip()

                c1, c2 = st.columns(2)
                with c1:
                    try:
                        idx_x = list_xa.index(val_xa)
                    except:
                        idx_x = 0
                    tt_xa = st.selectbox("Xã/Phường *", list_xa, index=idx_x, key="tt_xa")
                with c2:
                    tt_thon = st.text_input("Địa chỉ chi tiết *", value=str(val_thon), key="tt_thon")
                    st.caption("💡 Ghi chi tiết: số nhà, đường phố/thôn/xóm/tổ...")

                updated_values['Temp_XaPhuong_ThuongTru'] = tt_xa
                updated_values['Temp_ThonTo_ThuongTru'] = tt_thon
                updated_values[col] = f"{tt_thon}, {tt_xa}".strip(", ")

            # CÁC TRƯỜNG KHÁC
            else:
                clean_label = col
                for p in ["Nơi đăng ký khai sinh - ", "Quê quán (theo mô hình 2 cấp) - ", "Thường trú (theo mô hình 2 cấp) - "]:
                    clean_label = clean_label.replace(p, "")
                
                if col in OPTIONAL_COLS:
                    clean_label = clean_label.replace('*', '')

                if col in READ_ONLY_COLS:
                    st.text_input(clean_label, value=val, disabled=True, key=col)
                    updated_values[col] = str(val)
                elif col == 'Trạng thái hoạt động':
                    opts = ["Đang sinh hoạt Đảng", "Đã chuyển sinh hoạt"]
                    idx_opt = opts.index(val) if val in opts else 0
                    updated_values[col] = st.selectbox(clean_label, opts, index=idx_opt, key=col)
                elif col == 'Giới tính *':
                    opts = ["Nam", "Nữ"]
                    idx_opt = opts.index(val) if val in opts else 0
                    updated_values[col] = st.selectbox(clean_label, opts, index=idx_opt, key=col)
                else:
                    ph = "Để trống nếu chưa có thông tin" if col in OPTIONAL_COLS else ""
                    updated_values[col] = st.text_input(clean_label, value=str(val), placeholder=ph, key=col)

        st.write("---")
        
        # --- NÚT LƯU VÀ VALIDATION ---
        if st.button("💾 LƯU THÔNG TIN", type="primary", use_container_width=True):
            
            missing_fields = []

            # 1. CHECK KHAI SINH
            if updated_values.get('Nơi đăng ký khai sinh - Quốc gia *') == "Việt Nam":
                if not updated_values.get('Nơi đăng ký khai sinh - Tỉnh *'):
                    missing_fields.append("Khai sinh: Chưa chọn Tỉnh")
                if not str(updated_values.get('Temp_XaPhuong_KhaiSinh', '')).strip():
                    missing_fields.append("Khai sinh: Chưa chọn Xã/Phường")
                if not str(updated_values.get('Temp_ThonTo_KhaiSinh', '')).strip():
                    missing_fields.append("Khai sinh: Chưa nhập Thôn/Tổ/Số nhà")

            # 2. CHECK QUÊ QUÁN
            if updated_values.get('Quê quán (theo mô hình 2 cấp) - Quốc gia *') == "Việt Nam":
                if not updated_values.get('Quê quán (theo mô hình 2 cấp) - Tỉnh *'):
                    missing_fields.append("Quê quán: Chưa chọn Tỉnh")
                if not str(updated_values.get('Quê quán (theo mô hình 2 cấp) - Địa chỉ chi tiết *', '')).strip():
                    missing_fields.append("Quê quán: Chưa chọn Xã/Phường")

            # 3. CHECK THƯỜNG TRÚ
            if updated_values.get('Thường trú (theo mô hình 2 cấp) - Quốc gia *') == "Việt Nam":
                if not updated_values.get('Thường trú (theo mô hình 2 cấp) - Tỉnh *'):
                    missing_fields.append("Thường trú: Chưa chọn Tỉnh")
                if not str(updated_values.get('Temp_XaPhuong_ThuongTru', '')).strip():
                    missing_fields.append("Thường trú: Chưa chọn Xã/Phường")
                if not str(updated_values.get('Temp_ThonTo_ThuongTru', '')).strip():
                    missing_fields.append("Thường trú: Chưa nhập Thôn/Tổ/Số nhà")

            # 4. CHECK CÁC TRƯỜNG CÒN LẠI
            OTHER_REQUIRE = [
                'STT', 'ID', 'Họ và tên *', 'Giới tính *', 'Sinh ngày * (dd/mm/yyyy)',
                'Dân tộc *', 'Tôn giáo *', 'Số định danh cá nhân *',
                'Ngày vào Đảng* (dd/mm/yyyy)', 'Trạng thái hoạt động'
            ]
            
            for col_req in OTHER_REQUIRE:
                val_check = str(updated_values.get(col_req, "")).strip()
                if not val_check:
                    missing_fields.append(col_req.replace('*', ''))

            # --- XỬ LÝ KẾT QUẢ CHECK ---
            if missing_fields:
                st.error("⚠️ KHÔNG THỂ LƯU! Vui lòng điền đầy đủ các thông tin sau:", icon="🚫")
                for f in missing_fields:
                    st.markdown(f"- **{f}**")
            else:
                with st.spinner("💾 Đang lưu dữ liệu..."):
                    success = save_update_optimized(main_sheet, idx, updated_values, workbook)
                    
                    if success:
                        # Không cần force_refresh_data() vì đã xóa session trong save_update_optimized
                        st.session_state.step = 4
                        st.rerun()

        if st.button("Hủy bỏ"):
            st.session_state.step = 2
            st.rerun()

    # --- BƯỚC 4: THÀNH CÔNG ---
    elif st.session_state.step == 4:
        st.balloons()
        
        st.success("✅ CẬP NHẬT THÀNH CÔNG!", icon="✅")
        
        st.markdown("""
        <div style="padding: 20px; border: 1px solid #4CAF50; border-radius: 10px; background-color: #E8F5E9; color: #2E7D32;">
            <h3 style="margin:0">Dữ liệu đã được lưu an toàn.</h3>
            <p>Cảm ơn đồng chí đã cập nhật thông tin.</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.write("")
        st.write("")
        
        if st.button("⬅️ Quay về trang tìm kiếm", type="primary", use_container_width=True):
            st.session_state.step = 1
            st.session_state.selected_row_index = None
            st.session_state.search_results = None
            st.rerun()

# =========================================================
# CHẾ ĐỘ 2: ADMIN DASHBOARD
# =========================================================
elif app_mode == "📊 Admin Dashboard":
    st.title("📊 Thống kê Tiến độ Cập nhật")
    
    password = st.sidebar.text_input("Nhập mật khẩu Admin:", type="password")
    
    if password == ADMIN_PASSWORD:
        with st.spinner("📊 Đang tải thống kê..."):
            df_main, _, workbook = get_session_data()
            
            try:
                backup_sheet = workbook.worksheet(SHEET_NAME_BACKUP)
                updated_ids = get_updated_ids(backup_sheet)
            except gspread.exceptions.WorksheetNotFound:
                st.error("⚠️ Chưa có sheet Backup!")
                updated_ids = set()

            total_users = len(df_main)
            updated_count = df_main['ID'].isin(updated_ids).sum()
            not_updated_count = total_users - updated_count
            
            # --- HIỂN THỊ DASHBOARD ---
            col1, col2, col3 = st.columns(3)
            col1.metric("Tổng Đảng viên", f"{total_users} người")
            col2.metric("Đã cập nhật", f"{updated_count} người", delta=f"{updated_count/total_users*100:.1f}%")
            col3.metric("Chưa cập nhật", f"{not_updated_count} người", delta_color="inverse")
            
            st.progress(updated_count / total_users if total_users > 0 else 0)
            st.divider()

            # --- PHẦN 1: DANH SÁCH CHƯA CẬP NHẬT ---
            st.subheader(f"📋 Danh sách {not_updated_count} người CHƯA cập nhật")
            
            not_updated_df = df_main[~df_main['ID'].isin(updated_ids)].copy()
            
            display_cols = ['ID', 'Họ và tên *', 'Sinh ngày * (dd/mm/yyyy)', 'Tổ chức Đảng đang sinh hoạt * (không sửa)']
            st.dataframe(
                not_updated_df[display_cols],
                use_container_width=True,
                hide_index=True
            )

            # --- XUẤT FILE EXCEL ĐẦY ĐỦ ---
            buffer_missing = io.BytesIO()
            
            with pd.ExcelWriter(buffer_missing, engine='openpyxl') as writer:
                not_updated_df.to_excel(writer, index=False, sheet_name='ChuaCapNhat')
            
            buffer_missing.seek(0)
            
            file_name_missing = f"DS_ChuaCapNhat_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

            col_dl1, col_dl2 = st.columns([1, 2])
            with col_dl1:
                st.download_button(
                    label="📥 Tải danh sách đầy đủ (.xlsx)",
                    data=buffer_missing,
                    file_name=file_name_missing,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )

            st.divider()

            # --- PHẦN 2: TẢI FILE TỔNG HỢP ---
            st.subheader("🗄️ Xuất dữ liệu tổng hợp đầy đủ")
            st.write("Tải về file Excel chứa toàn bộ dữ liệu mới nhất từ hệ thống.")

            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_main.to_excel(writer, index=False, sheet_name='DanhSachTongHop')
            
            buffer.seek(0)

            file_name_excel = f"TongHop_DangVien_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

            st.download_button(
                label="📥 Tải trọn bộ dữ liệu (Excel .xlsx)",
                data=buffer,
                file_name=file_name_excel,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
    elif password:
        st.error("❌ Sai mật khẩu!")
    else:
        st.info("🔒 Vui lòng nhập mật khẩu để xem thống kê.")
