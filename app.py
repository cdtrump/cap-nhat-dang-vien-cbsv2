import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import io
import time
from functools import wraps

# --- CẤU HÌNH ---
ADMIN_PASSWORD = st.secrets["admin_password"]
CACHE_TTL = 30  


# --- CẤU HÌNH ---
# Danh sách cột CHÍNH XÁC (33 cột)
ALL_COLUMNS = [
    'STT', 
    'ID', 
    'Họ và tên *', 
    'Tên gọi khác', 
    'Giới tính *', 
    'Sinh ngày * (dd/mm/yyyy)',
    'Dân tộc *', 
    'Tôn giáo *', 
    'Số định danh cá nhân *', 
    'Số thẻ Đảng* (12 số theo HD38-HD/BTCTW)',
    'Nơi cấp thẻ Đảng', 
    'Ngày cấp thẻ Đảng (dd/mm/yyyy)', 
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
    
    # --- CỘT NÀY QUAN TRỌNG: Cần giữ lại để giữ chỗ, dù không dùng ---
    'Đề nghị xóa (do đang viên không thuộc chi bộ)/ (Nếu muốn xóa chọn "có", còn không bỏ qua)',
    
    # --- 4 CỘT PHỤ MỚI THÊM ---
    'Temp_XaPhuong_KhaiSinh', 
    'Temp_ThonTo_KhaiSinh', 
    'Temp_XaPhuong_ThuongTru', 
    'Temp_ThonTo_ThuongTru',
    'Ghi chú'
]

# Danh sách cột phụ
TEMP_COLS = ['Temp_XaPhuong_KhaiSinh', 'Temp_ThonTo_KhaiSinh', 'Temp_XaPhuong_ThuongTru', 'Temp_ThonTo_ThuongTru', 'Ghi chú', 'Đề nghị xóa (do đang viên không thuộc chi bộ)/ (Nếu muốn xóa chọn "có", còn không bỏ qua)']

# Cột này chỉ đọc, không cho sửa
READ_ONLY_COLS = [
    'STT', 'ID', 'Họ và tên *', 'Sinh ngày * (dd/mm/yyyy)', 
    'Tổ chức Đảng đang sinh hoạt * (không sửa)'
]

SHEET_NAME_MAIN = "Sheet1"
SHEET_NAME_BACKUP = "Backup"

# --- HÀM KẾT NỐI ---
@st.cache_resource
def connect_to_workbook():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
    except:
        import json
        key_dict = json.loads(st.secrets["textkey"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    client = gspread.authorize(creds)
    return client.open("DanhSachDangVien")

# ========================================
# 🔥 GIẢI PHÁP AUTO-RETRY KHI GẶP LỖI 429
# ========================================

def retry_on_rate_limit(max_retries=5, initial_wait=2):
    """Decorator tự động retry khi gặp lỗi 429 (Rate Limit)"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            wait_time = initial_wait
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except gspread.exceptions.APIError as e:
                    if e.response.status_code == 429:
                        if attempt < max_retries - 1:
                            with st.spinner(f"⏳ Hệ thống đang bận, chờ {wait_time}s... (Lần {attempt + 1}/{max_retries})"):
                                time.sleep(wait_time)
                            wait_time *= 2
                        else:
                            st.error("❌ Hệ thống quá tải. Vui lòng thử lại sau 1 phút.")
                            raise
                    else:
                        raise
                except Exception as e:
                    st.error(f"⚠️ Lỗi không xác định: {str(e)}")
                    raise
            return None
        return wrapper
    return decorator

# --- CÁC HÀM WRAPPER AN TOÀN (CẬP NHẬT ĐẦY ĐỦ) ---

@retry_on_rate_limit()
def safe_get_all_records(sheet, expected_headers):
    return sheet.get_all_records(expected_headers=expected_headers)

@retry_on_rate_limit()
def safe_update_sheet(sheet, cell_range, values):
    return sheet.update(cell_range, values, value_input_option='USER_ENTERED')

@retry_on_rate_limit()
def safe_append_row(sheet, row_data):
    return sheet.append_row(row_data, value_input_option='USER_ENTERED')

# --- ĐÂY LÀ HÀM BẠN ĐANG THIẾU ---
@retry_on_rate_limit()
def safe_get_all_values(sheet):
    return sheet.get_all_values()
# ---------------------------------

@retry_on_rate_limit()
def safe_find_cell(sheet, value, in_column):
    return sheet.find(value, in_column=in_column)

# ========================================
# ✅ CACHING & STATE MANAGEMENT (1 PHÚT)
# ========================================

@st.cache_data(ttl=CACHE_TTL)
def load_data_main_cached(_sheet):
    """Load data có cache 1 phút, xử lý số 0 ở đầu"""
    data = safe_get_all_records(_sheet, ALL_COLUMNS)
    df = pd.DataFrame(data)
    
    # Xử lý số 0 ở đầu (Logic cũ nhưng đưa vào cache)
    cols_need_zero = ['Số định danh cá nhân *', 'Số thẻ Đảng* (12 số theo HD38-HD/BTCTW)', 'Số CMND cũ (nếu có)']
    for col in cols_need_zero:
        if col in df.columns:
            df[col] = df[col].astype(str).replace(r'\.0$', '', regex=True).replace(['nan', 'None', ''], '')
            df[col] = df[col].apply(lambda x: x.zfill(12) if x.strip() != '' and x.isdigit() else x)
            
    df['ID'] = df['ID'].astype(str).replace(r'\.0$', '', regex=True)
    return df

def init_session_data():
    """Khởi tạo session state nếu chưa có"""
    if 'data_loaded' not in st.session_state:
        with st.spinner("🔄 Đang tải dữ liệu..."):
            workbook = connect_to_workbook()
            sheet = workbook.worksheet(SHEET_NAME_MAIN)
            df = load_data_main_cached(sheet)
            
            st.session_state.df_main = df
            st.session_state.main_sheet = sheet
            st.session_state.workbook = workbook
            st.session_state.data_loaded = True
            st.session_state.last_load_time = time.time()

def get_session_data():
    """Hàm duy nhất để lấy dữ liệu trong app"""
    init_session_data()
    return st.session_state.df_main, st.session_state.main_sheet, st.session_state.workbook

def force_refresh_data():
    """Admin dùng để xóa cache và tải lại ngay lập tức"""
    st.cache_data.clear()
    for key in ['data_loaded', 'df_main', 'main_sheet', 'workbook', 'last_load_time']:
        if key in st.session_state:
            del st.session_state[key]
    init_session_data()

# ---  ---

def normalize_province_name(name):
    """
    Chuẩn hóa tên tỉnh/thành phố để so sánh linh hoạt.
    Ví dụ: "Thành phố Hải Phòng" -> "hải phòng"
             "Hải Phòng" -> "hải phòng"
             "Tỉnh Hà Giang" -> "hà giang"
    """
    if not isinstance(name, str):
        return ""
        
    name = name.lower() # 1. Chuyển về chữ thường
    
    # 2. Loại bỏ các tiền tố phổ biến
    prefixes_to_remove = ["thành phố ", "tp. ", "tp ", "tỉnh "]
    for prefix in prefixes_to_remove:
        if name.startswith(prefix):
            name = name.replace(prefix, "", 1) # Chỉ thay thế 1 lần ở đầu
            break
            
    return name.strip()

def find_province_index(province_from_sheet, all_provinces_list):
    """
    Tìm chỉ mục (index) của một tỉnh trong danh sách một cách linh hoạt.
    Trả về chỉ mục nếu tìm thấy, ngược lại trả về 0 (giá trị mặc định).
    """
    normalized_target = normalize_province_name(province_from_sheet)
    
    if not normalized_target:
        return 0

    for index, province_from_json in enumerate(all_provinces_list):
        normalized_json_province = normalize_province_name(province_from_json)
        if normalized_target == normalized_json_province:
            return index
            
    return 0 # Không tìm thấy, trả về index đầu tiên
    
def save_update_optimized(sheet, row_index, updated_values, workbook):
    try:
        # 1. Xử lý format Text cho Google Sheet (thêm dấu ' )
        cols_force_text = [
            'ID',
            'Số định danh cá nhân *', 
            'Số thẻ Đảng* (12 số theo HD38-HD/BTCTW)',
            'Số thẻ theo Đảng quyết định 85',
            'Số CMND cũ (nếu có)',
            'Sinh ngày * (dd/mm/yyyy)',
            'Ngày cấp thẻ Đảng (dd/mm/yyyy)',
            'Ngày vào Đảng* (dd/mm/yyyy)', 
            'Ngày vào Đảng chính thức* (dd/mm/yyyy)',
            'Ngày rời khỏi/ Ngày mất/ Ngày miễn sinh hoạt Đảng (dd/mm/yyyy)'
        ]

        row_vals = []
        for col in ALL_COLUMNS:
            val = updated_values.get(col, "")
            if col in cols_force_text and val:
                val = "'" + str(val)
            row_vals.append(val)
        
        # 2. Backup (An toàn - Giờ VN)
        try:
            backup_sheet = workbook.worksheet(SHEET_NAME_BACKUP)
            vn_time = (datetime.utcnow() + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S")
            safe_append_row(backup_sheet, [vn_time] + row_vals)
        except: pass
        
        # 3. GHI LÊN GOOGLE SHEET (Dùng tìm kiếm ID an toàn)
        target_id = str(updated_values.get('ID', '')).strip()
        found_cell = safe_find_cell(sheet, target_id, in_column=2)
        
        if found_cell:
            safe_update_sheet(sheet, f"A{found_cell.row}", [row_vals])
        else:
            st.error(f"❌ Không tìm thấy ID {target_id} trong file gốc!")
            return False
        
        # ========================================================
        # 🔥 4. CẬP NHẬT NÓNG VÀO SESSION (QUAN TRỌNG)
        # Thay vì xóa session, ta sửa trực tiếp dữ liệu trong bộ nhớ
        # để User A thấy kết quả ngay lập tức mà không cần chờ Cache
        # ========================================================
        if 'df_main' in st.session_state:
            # Lặp qua từng cột để cập nhật giá trị mới vào DataFrame
            for col in ALL_COLUMNS:
                # Lấy giá trị trần (không có dấu ' ) để hiển thị trên Web cho đẹp
                raw_val = updated_values.get(col, "")
                st.session_state.df_main.at[row_index, col] = raw_val
            
            # Đặt lại thời gian tải để Session này không bị coi là hết hạn ngay
            st.session_state.last_load_time = time.time()
            
            # Đảm bảo cờ data_loaded vẫn còn
            st.session_state.data_loaded = True

        return True

    except Exception as e:
        st.error(f"❌ Lỗi lưu dữ liệu: {str(e)}")
        return False

# --- GIAO DIỆN CHÍNH ---
st.set_page_config(page_title="Cập nhật thông tin Đảng viên CBSV II -NEU", layout="wide")
st.markdown("""
    <style>
    /* Chỉ áp dụng khi màn hình nhỏ hơn 768px (Điện thoại dọc) */
    @media only screen and (max-width: 768px) {
        
        /* 1. Chỉnh lại container chính để không bị che bởi thanh menu trên cùng */
        .block-container {
            padding-top: 4.5rem !important; /* Tăng từ 2rem lên 4.5rem */
            padding-left: 1rem !important;
            padding-right: 1rem !important;
        }
        
        /* 2. Thu nhỏ tiêu đề chính (H1) */
        h1 {
            font-size: 1.6rem !important; /* Giảm thêm chút nữa cho gọn */
            padding-top: 0rem !important;
        }
        
        /* 3. Thu nhỏ tiêu đề phụ (H2, H3) */
        h2 {
            font-size: 1.3rem !important;
        }
        h3 {
            font-size: 1.1rem !important;
        }
        
        /* 4. Thu nhỏ chữ trong ô nhập liệu và nhãn */
        .stTextInput label, .stSelectbox label {
            font-size: 0.9rem !important;
        }
        .stTextInput input {
            font-size: 0.9rem !important;
        }
        
        /* 5. Chỉnh nút bấm */
        .stButton button {
            font-size: 1rem !important;
            width: 100% !important; /* Cho nút bấm full chiều ngang bấm cho dễ */
        }
    }
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR MENU ---
st.sidebar.title("Menu")
app_mode = st.sidebar.radio("Chọn chức năng:", ["👤 Cập nhật thông tin", "📊 Admin Dashboard"])

# =========================================================
# CHẾ ĐỘ 1: NGƯỜI DÙNG CẬP NHẬT
# =========================================================
if app_mode == "👤 Cập nhật thông tin":
    st.title("📝 Cập nhật thông tin Đảng viên CBSV II -NEU")
    
    # Khởi tạo state nếu chưa có
    if 'step' not in st.session_state:
        st.session_state.step = 1
    if 'selected_row_index' not in st.session_state:
        st.session_state.selected_row_index = None

# --- STEP 1: SEARCH ---
    if st.session_state.step == 1:
        st.subheader("Bước 1: Tra cứu thông tin")
        
        # Initialize search mode state if not present
        if 'search_mode' not in st.session_state:
            st.session_state.search_mode = 'id'  # Default to ID search

        # --- MODE 1: SEARCH BY ID (Preferred) ---
        if st.session_state.search_mode == 'id':
            with st.form("search_id_form"):
                st.markdown("#### 🔍 Tra cứu bằng Số định danh cá nhân (CCCD/ĐDCN)")
                search_id = st.text_input("Nhập Số định danh cá nhân (12 số):", placeholder="Ví dụ: 030098123456")
                submitted_id = st.form_submit_button("Tra cứu ngay", type="primary")

                if submitted_id:
                    if not search_id:
                        st.warning("Vui lòng nhập Số định danh cá nhân.")
                    else:
                        with st.spinner("Đang tìm kiếm theo số định danh..."):
                            df, _, _ = get_session_data()
                            
                            # Normalize input and data for comparison (remove spaces, ensure string)
                            clean_input_id = search_id.strip()
                            
                            # Ensure the column is treated as string for comparison
                            # Note: 'Số định danh cá nhân *' is the exact column name
                            mask = df['Số định danh cá nhân *'].astype(str).str.strip() == clean_input_id
                            results = df[mask]

                            if not results.empty:
                                st.success(f"✅ Tìm thấy thông tin của: {results.iloc[0]['Họ và tên *']}")
                                st.session_state.search_results = results
                                st.session_state.step = 2
                                st.rerun()
                            else:
                                st.error(f"❌ Không tìm thấy số định danh: {clean_input_id}")
                                # Enable fallback option
                                st.session_state.show_name_search_option = True

            # Show button to switch to Name search if ID search fails or user wants to switch
            if st.session_state.get('show_name_search_option', False):
                st.info("Không tìm thấy? Có thể số định danh chưa được cập nhật chính xác.")
                if st.button("👉 Thử tìm bằng Họ Tên và Ngày Sinh"):
                    st.session_state.search_mode = 'name'
                    st.rerun()
            
            # Optional: Link to switch mode manually if they don't have ID handy
            elif st.button("Chuyển sang tìm bằng Họ Tên & Ngày Sinh"):
                st.session_state.search_mode = 'name'
                st.rerun()

        # --- MODE 2: SEARCH BY NAME & DOB (Fallback) ---
        elif st.session_state.search_mode == 'name':
            with st.form("search_name_form"):
                st.markdown("#### 👤 Tra cứu bằng Họ Tên và Ngày Sinh")
                col_s1, col_s2 = st.columns(2)
                with col_s1:
                    search_name = st.text_input("Họ và tên (đầy đủ có dấu):")
                with col_s2:
                    search_dob = st.text_input("Ngày sinh (dd/mm/yyyy):", placeholder="Ví dụ: 05/01/2005")
                
                submitted_name = st.form_submit_button("Tra cứu", type="primary")

                if submitted_name:
                    if not search_name or not search_dob:
                        st.warning("Vui lòng nhập đầy đủ Họ tên và Ngày sinh.")
                    else:
                        with st.spinner("Đang tìm kiếm..."):
                            df, _, _ = get_session_data()
                            # Case-insensitive search
                            mask = (
                                df['Họ và tên *'].str.strip().str.lower() == search_name.strip().lower()
                            ) & (
                                df['Sinh ngày * (dd/mm/yyyy)'] == search_dob.strip()
                            )
                            results = df[mask]

                            if results.empty:
                                st.error("❌ Không tìm thấy thông tin.")
                                st.info("Lưu ý: Kiểm tra kỹ chính tả tiếng Việt và định dạng ngày (dd/mm/yyyy).")
                            else:
                                st.success(f"Tìm thấy {len(results)} kết quả.")
                                st.session_state.search_results = results
                                st.session_state.step = 2
                                st.rerun()
            
            # Button to go back to ID search
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
                    # Lưu index thực của dòng trong DataFrame gốc
                    if st.button("CẬP NHẬT", key=f"btn_{index}", type="primary"):
                        st.session_state.selected_row_index = index
                        st.session_state.step = 3
                        st.rerun()
        
        st.write("---")
        if st.button("⬅️ Quay lại tìm kiếm"):
            st.session_state.step = 1
            st.rerun()

# --- BƯỚC 3: CẬP NHẬT THÔNG TIN (INTERACTIVE MODE) ---
    elif st.session_state.step == 3:

        
        # 1. Load Data Địa chính
        import json
        @st.cache_data
        def load_location_data():
            try:
                with open('vietnam_data.json', 'r', encoding='utf-8') as f:
                    return json.load(f)
            except FileNotFoundError: return {}

        vn_locations = load_location_data()
        list_tinh = list(vn_locations.keys())
        
        # 2. Load Data User
        df, main_sheet, workbook = get_session_data()
        idx = st.session_state.selected_row_index
        
        try:
            current_data = df.loc[idx]
        except KeyError:
            st.error("Phiên làm việc hết hạn."); st.stop()

        note_content = str(current_data.get('Ghi chú', '')).strip()
        if note_content:
            st.error(f"⚠️ Ghi chú từ Chi ủy: {note_content}", icon="📢")
        # ==================================================

        st.subheader("Bước 3: Cập nhật thông tin chi tiết")

        st.write("Kiểm tra và chỉnh sửa các thông tin dưới đây:")
        
        updated_values = {}

        # Danh sách Optional
        OPTIONAL_COLS = [
            'Số thẻ Đảng* (12 số theo HD38-HD/BTCTW)', 'Ngày cấp thẻ Đảng (dd/mm/yyyy)',
            'Số thẻ theo Đảng quyết định 85', 'Ngày vào Đảng chính thức* (dd/mm/yyyy)',
            'Nơi cấp thẻ Đảng', 'Số CMND cũ (nếu có)', 'Tên gọi khác'
        ]

        # --- BẮT ĐẦU VÒNG LẶP HIỂN THỊ FORM ---
        for col in ALL_COLUMNS:
            if col in TEMP_COLS: continue
            
            val = current_data.get(col, "")

            # ========================================================
            # 1. KHAI SINH
            # ========================================================
            if col == 'Nơi đăng ký khai sinh - Quốc gia *':
                st.markdown("---"); st.subheader("🏠 THÔNG TIN KHAI SINH")
                is_russia = str(val).strip().upper() in ["LIÊN BANG NGA", "NGA", "RUSSIA"]
                ks_quocgia = st.radio("Quốc gia *", ["Việt Nam", "Liên Bang Nga"], index=1 if is_russia else 0, horizontal=True, key="ks_qg")
                updated_values[col] = ks_quocgia

            elif col == 'Nơi đăng ký khai sinh - Tỉnh *':
                cur_qg = st.session_state.get("ks_qg", "Việt Nam")
                if cur_qg == "Liên Bang Nga":
                    st.text_input("Tỉnh *", value="KHÔNG", disabled=True, key="ks_tinh_nga")
                    updated_values[col] = "KHÔNG"
                else:
                    # SỬA Ở ĐÂY: Dùng hàm tìm kiếm linh hoạt
                    idx = find_province_index(str(val), list_tinh)
                    ks_tinh = st.selectbox("Tỉnh *", list_tinh, index=idx, key="ks_tinh_vn")
                    updated_values[col] = ks_tinh

            elif col == 'Nơi đăng ký khai sinh - Địa chỉ chi tiết *':
                cur_qg = st.session_state.get("ks_qg", "Việt Nam")
                if cur_qg == "Liên Bang Nga":
                    c1, c2 = st.columns(2)
                    with c1: st.text_input("Xã/Phường/ Đặc khu *", value="KHÔNG", disabled=True, key="ks_xa_nga")
                    with c2: st.text_input("Địa chỉ chi tiết (Thôn/Tổ...)*", value="KHÔNG", disabled=True, key="ks_thon_nga")
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
                        if len(parts) >= 2: val_xa = parts[-1].strip(); val_thon = ",".join(parts[:-1]).strip()

                    c1, c2 = st.columns(2)
                    with c1:
                        try: idx = list_xa.index(val_xa)
                        except: idx = 0
                        input_xa = st.selectbox("Xã/Phường/ Đặc khu *", list_xa, index=idx, key="ks_xa_vn")
                    with c2:
                        input_thon = st.text_input("Địa chỉ chi tiết dưới Xã/Phường/ Đặc khu *", value=str(val_thon), key="ks_thon_vn")
                    
                    updated_values['Temp_XaPhuong_KhaiSinh'] = input_xa
                    updated_values['Temp_ThonTo_KhaiSinh'] = input_thon
                    updated_values[col] = f"{input_thon}, {input_xa}".strip(", ")

            # ========================================================
            # 2. QUÊ QUÁN
            # ========================================================
            elif col == 'Quê quán (theo mô hình 2 cấp) - Quốc gia *':
                st.markdown("---"); st.subheader("🏠 THÔNG TIN QUÊ QUÁN")
                st.text_input("Quốc gia *", value="Việt Nam", disabled=True, key="qq_qg")
                updated_values[col] = "Việt Nam"

            elif col == 'Quê quán (theo mô hình 2 cấp) - Tỉnh *':
                # SỬA Ở ĐÂY
                idx = find_province_index(str(val), list_tinh)
                qq_tinh = st.selectbox("Tỉnh *", list_tinh, index=idx, key="qq_tinh")
                updated_values[col] = qq_tinh

            elif col == 'Quê quán (theo mô hình 2 cấp) - Địa chỉ chi tiết *':
                cur_tinh = st.session_state.get("qq_tinh", "")
                list_xa = vn_locations.get(cur_tinh, [])
                try: idx = list_xa.index(str(val))
                except: idx = 0
                qq_xa = st.selectbox("Xã/Phường/ Đặc khu *", list_xa, index=idx, key="qq_xa")
                updated_values[col] = qq_xa

            # ========================================================
            # 3. THƯỜNG TRÚ
            # ========================================================
            elif col == 'Thường trú (theo mô hình 2 cấp) - Quốc gia *':
                st.markdown("---"); st.subheader("🏠 THÔNG TIN THƯỜNG TRÚ")
                st.text_input("Quốc gia *", value="Việt Nam", disabled=True, key="tt_qg")
                updated_values[col] = "Việt Nam"

            elif col == 'Thường trú (theo mô hình 2 cấp) - Tỉnh *':
                # SỬA Ở ĐÂY
                idx = find_province_index(str(val), list_tinh)
                tt_tinh = st.selectbox("Tỉnh *", list_tinh, index=idx, key="tt_tinh")
                updated_values[col] = tt_tinh

            elif col == 'Thường trú (theo mô hình 2 cấp) - Địa chỉ chi tiết *':
                cur_tinh = st.session_state.get("tt_tinh", "")
                list_xa = vn_locations.get(cur_tinh, [])
                
                val_xa = current_data.get('Temp_XaPhuong_ThuongTru', '')
                val_thon = current_data.get('Temp_ThonTo_ThuongTru', '')
                if not val_xa and str(val):
                    parts = str(val).split(',')
                    if len(parts) >= 2: val_xa = parts[-1].strip(); val_thon = ",".join(parts[:-1]).strip()

                c1, c2 = st.columns(2)
                with c1:
                    try: idx = list_xa.index(val_xa)
                    except: idx = 0
                    tt_xa = st.selectbox("Xã/Phường/ Đặc khu *", list_xa, index=idx, key="tt_xa")
                with c2:
                    tt_thon = st.text_input("Địa chỉ chi tiết dưới Xã/Phường/ Đặc khu *", value=str(val_thon), key="tt_thon")
                    st.caption("💡 Cách ghi: ghi chi tiết nhất có thể, bao gồm: số nhà, đường phố/thôn/xóm/tổ... (ví dụ Thôn Hòa Bình Hạ/ Tổ dân số 5/ Số 60 Ngách 6/12 Đội Nhân)")

                updated_values['Temp_XaPhuong_ThuongTru'] = tt_xa
                updated_values['Temp_ThonTo_ThuongTru'] = tt_thon
                updated_values[col] = f"{tt_thon}, {tt_xa}".strip(", ")

            # ========================================================
            # CÁC TRƯỜNG KHÁC
            # ========================================================
            else:
                clean_label = col
                for p in ["Nơi đăng ký khai sinh - ", "Quê quán (theo mô hình 2 cấp) - ", "Thường trú (theo mô hình 2 cấp) - "]:
                    clean_label = clean_label.replace(p, "")
                
                if col in OPTIONAL_COLS: clean_label = clean_label.replace('*', '')

                if col in READ_ONLY_COLS:
                    st.text_input(clean_label, value=val, disabled=True, key=col)
                    updated_values[col] = str(val)
                elif col == 'Trạng thái hoạt động':
                    opts = ["Đang sinh hoạt Đảng", "Đã chuyển sinh hoạt"]
                    idx = opts.index(val) if val in opts else 0
                    updated_values[col] = st.selectbox(clean_label, opts, index=idx, key=col)
                elif col == 'Giới tính *':
                    opts = ["Nam", "Nữ"]
                    idx = opts.index(val) if val in opts else 0
                    updated_values[col] = st.selectbox(clean_label, opts, index=idx, key=col)
                else:
                    ph = "Để trống nếu chưa có thông tin" if col in OPTIONAL_COLS else ""
                    updated_values[col] = st.text_input(clean_label, value=str(val), placeholder=ph, key=col)

        st.write("---")
        
        # --- NÚT LƯU VÀ VALIDATION (NÂNG CẤP CHECK RIÊNG LẺ) ---
        if st.button("💾 LƯU THÔNG TIN", type="primary", use_container_width=True):
            
            updated_values['Ghi chú'] = current_data.get('Ghi chú', '')
            col_xoa = 'Đề nghị xóa (do đang viên không thuộc chi bộ)/ (Nếu muốn xóa chọn "có", còn không bỏ qua)'
            updated_values[col_xoa] = current_data.get(col_xoa, "")

            missing_fields = []

            # 1. CHECK KHAI SINH (Kiểm tra kỹ từng thành phần)
            if updated_values.get('Nơi đăng ký khai sinh - Quốc gia *') == "Việt Nam":
                if not updated_values.get('Nơi đăng ký khai sinh - Tỉnh *'): 
                    missing_fields.append("Khai sinh: Chưa chọn Tỉnh")
                # Check Xã (Temp)
                if not str(updated_values.get('Temp_XaPhuong_KhaiSinh', '')).strip(): 
                    missing_fields.append("Khai sinh: Chưa chọn Xã/Phường")
                # Check Thôn (Temp)
                if not str(updated_values.get('Temp_ThonTo_KhaiSinh', '')).strip(): 
                    missing_fields.append("Khai sinh: Chưa nhập Thôn/Tổ/Số nhà")

            # 2. CHECK QUÊ QUÁN
            if updated_values.get('Quê quán (theo mô hình 2 cấp) - Quốc gia *') == "Việt Nam":
                if not updated_values.get('Quê quán (theo mô hình 2 cấp) - Tỉnh *'): 
                    missing_fields.append("Quê quán: Chưa chọn Tỉnh")
                # Quê quán chỉ cần Xã (check trực tiếp giá trị cột chính vì không có cột Temp riêng cho Xã Quê Quán trong logic cũ)
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

            # 4. CHECK CÁC TRƯỜNG CÒN LẠI (Dùng danh sách REQUIRE cũ)
            OTHER_REQUIRE = [
                'Họ và tên *', 'Giới tính *', 'Sinh ngày * (dd/mm/yyyy)',
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
                for f in missing_fields: st.markdown(f"- **{f}**")
            else:
                with st.spinner("💾 Đang lưu dữ liệu..."):
                    success = save_update_optimized(main_sheet, idx, updated_values, workbook)
                    
                    if success:
                        st.session_state.step = 4
                        st.rerun()

        if st.button("Hủy bỏ"):
            st.session_state.step = 2
            st.rerun()

    # --- BƯỚC 4: MÀN HÌNH THÔNG BÁO THÀNH CÔNG (MỚI) ---
    elif st.session_state.step == 4:
        st.balloons() # Hiệu ứng pháo giấy
        
        st.success("✅ CẬP NHẬT THÀNH CÔNG!", icon="✅")
        
        st.markdown("""
        <div style="padding: 20px; border: 1px solid #4CAF50; border-radius: 10px; background-color: #E8F5E9; color: #2E7D32;">
            <h3 style="margin:0">Dữ liệu đã được lưu an toàn.</h3>
            <p>Cảm ơn đồng chí đã cập nhật thông tin.</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.write("")
        st.write("")
        
        if st.button("⬅️ Quay về trang tìm kiếm để cập nhật người khác", type="primary", use_container_width=True):
            # Reset toàn bộ session để về trạng thái ban đầu
            st.session_state.step = 1
            st.session_state.selected_row_index = None
            st.session_state.search_results = None
            st.rerun()

# =========================================================
# CHẾ ĐỘ 2: ADMIN DASHBOARD
# =========================================================
elif app_mode == "📊 Admin Dashboard":
    st.title("📊 Thống kê Tiến độ Cập nhật")
    
    # 1. Hiển thị ô nhập mật khẩu trước tiên
    password = st.sidebar.text_input("Nhập mật khẩu Admin:", type="password")
    
    # 2. Chỉ khi đúng mật khẩu mới hiện các chức năng quản lý
    if password == ADMIN_PASSWORD:
        
        # --- KHU VỰC TRẠNG THÁI CACHE (Đã chuyển vào trong) ---
        st.sidebar.divider()
        st.sidebar.markdown("### 📊 Trạng thái dữ liệu")
        
        # Logic hiển thị trạng thái cache
        if 'last_load_time' in st.session_state:
            elapsed = int(time.time() - st.session_state.last_load_time)
            mins, secs = divmod(elapsed, 60)
            st.sidebar.caption(f"⏱️ Cache: {mins}p {secs}s trước (Tự làm mới sau 1p)")
            
            # Nút làm mới (Chỉ Admin mới bấm được)
            if st.sidebar.button("🔄 Làm mới ngay"):
                force_refresh_data()
                st.rerun()
        else:
            st.sidebar.info("Dữ liệu đang được tải...")
        # ------------------------------------------------------

        with st.spinner("Đang tải dữ liệu thống kê..."):
            # Load dữ liệu mới nhất từ Sheet1
            df_main, _, workbook = get_session_data()
            
            try:
                backup_sheet = workbook.worksheet(SHEET_NAME_BACKUP)
                backup_rows = safe_get_all_values(backup_sheet)
                if len(backup_rows) > 1:
                    updated_ids = set([str(row[2]).replace('.0', '') for row in backup_rows[1:] if len(row) > 2])
                else:
                    updated_ids = set()
            except gspread.exceptions.WorksheetNotFound:
                st.error("Chưa có sheet Backup!")
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
            
            # Lọc ra những người chưa cập nhật
            not_updated_df = df_main[~df_main['ID'].isin(updated_ids)].copy()
            
            # Hiển thị trên web
            display_cols = ['ID', 'Họ và tên *', 'Sinh ngày * (dd/mm/yyyy)', 'Tổ chức Đảng đang sinh hoạt * (không sửa)']
            st.dataframe(
                not_updated_df[display_cols],
                use_container_width=True,
                hide_index=True
            )

            # --- XỬ LÝ XUẤT FILE EXCEL ĐẦY ĐỦ (CẬP NHẬT: MASK THÁNG SINH & ÍT CỘT) ---
            # Tạo bộ nhớ đệm cho file Excel
            buffer_missing = io.BytesIO()
            
            # 1. Chọn các cột cần xuất
            export_cols = ['ID', 'Họ và tên *', 'Sinh ngày * (dd/mm/yyyy)']
            # Tạo bản sao để không ảnh hưởng dữ liệu gốc
            export_df = not_updated_df[export_cols].copy()

            # 2. Hàm xử lý che tháng sinh (dd/mm/yyyy -> dd/**/yyyy)
            def mask_month_date(val):
                val = str(val).strip()
                parts = val.split('/')
                if len(parts) == 3:
                    # parts[0]=ngày, parts[1]=tháng, parts[2]=năm
                    return f"{parts[0]}/**/{parts[2]}"
                return val

            # 3. Áp dụng che tháng cho cột ngày sinh
            export_df['Sinh ngày * (dd/mm/yyyy)'] = export_df['Sinh ngày * (dd/mm/yyyy)'].apply(mask_month_date)

            # 4. Ghi ra Excel
            with pd.ExcelWriter(buffer_missing, engine='openpyxl') as writer:
                export_df.to_excel(writer, index=False, sheet_name='ChuaCapNhat')
            
            # Đưa con trỏ về đầu file
            buffer_missing.seek(0)
            
            # Tên file kèm thời gian
            vn_filename_time = (datetime.utcnow() + timedelta(hours=7)).strftime('%Y%m%d_%H%M')
            file_name_missing = f"DS_ChuaCapNhat_RUTGON_{vn_filename_time}.xlsx"

            col_dl1, col_dl2 = st.columns([1, 2])
            with col_dl1:
                st.download_button(
                    label="📥 Tải danh sách rút gọn (.xlsx)",
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

            vn_filename_time = (datetime.utcnow() + timedelta(hours=7)).strftime('%Y%m%d_%H%M')
            file_name_excel = f"TongHop_DangVien_{vn_filename_time}.xlsx"

            st.download_button(
                label="📥 Tải trọn bộ dữ liệu (Excel .xlsx)",
                data=buffer,
                file_name=file_name_excel,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
    elif password:
        st.error("Sai mật khẩu!")
    else:
        st.info("Vui lòng nhập mật khẩu để xem thống kê.")






