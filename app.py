import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import io

# --- CẤU HÌNH ---
ADMIN_PASSWORD = st.secrets["admin_password"]


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
    'Temp_ThonTo_ThuongTru'
]

# Danh sách cột phụ
TEMP_COLS = ['Temp_XaPhuong_KhaiSinh', 'Temp_ThonTo_KhaiSinh', 'Temp_XaPhuong_ThuongTru', 'Temp_ThonTo_ThuongTru']

# Cột này chỉ đọc, không cho sửa
READ_ONLY_COLS = [
    'STT', 'ID', 'Họ và tên *', 'Sinh ngày * (dd/mm/yyyy)', 
    'Tổ chức Đảng đang sinh hoạt * (không sửa)',
    # Thêm cột rác này vào readonly để user không quan tâm
    'Đề nghị xóa (do đang viên không thuộc chi bộ)/ (Nếu muốn xóa chọn "có", còn không bỏ qua)'
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

def load_data_main():
    workbook = connect_to_workbook()
    sheet = workbook.worksheet(SHEET_NAME_MAIN)
    
    # Lấy toàn bộ giá trị dưới dạng chuỗi (để tránh Google tự convert sang số)
    # Tuy nhiên get_all_records đôi khi vẫn tự convert, nên ta cần xử lý kỹ ở bước DataFrame
    data = sheet.get_all_records(expected_headers=ALL_COLUMNS)
    df = pd.DataFrame(data)
    
    # --- XỬ LÝ SỐ 0 Ở ĐẦU ---
    # Danh sách các cột cần đảm bảo là chuỗi và có số 0
    cols_need_zero = [
        'Số định danh cá nhân *', 
        'Số thẻ Đảng* (12 số theo HD38-HD/BTCTW)',
        'Số CMND cũ (nếu có)'
    ]
    
    for col in cols_need_zero:
        if col in df.columns:
            # Bước 1: Ép về kiểu chuỗi, xử lý lỗi .0 (ví dụ 123.0 -> 123)
            df[col] = df[col].astype(str).replace(r'\.0$', '', regex=True)
            
            # Bước 2: Thay thế 'nan' hoặc chuỗi rỗng bằng ''
            df[col] = df[col].replace(['nan', 'None', ''], '')
            
            # Bước 3: Nếu có dữ liệu (khác rỗng), thêm số 0 vào đầu cho đủ 12 ký tự
            # Lưu ý: Chỉ fill nếu nó là chuỗi số. Nếu đang trống thì giữ nguyên.
            df[col] = df[col].apply(lambda x: x.zfill(12) if x.strip() != '' and x.isdigit() else x)

    # Ép kiểu ID về string để so sánh trong logic tìm kiếm
    df['ID'] = df['ID'].astype(str).replace(r'\.0$', '', regex=True)
    
    return df, sheet, workbook

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
                            df, _, _ = load_data_main()
                            
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
                            df, _, _ = load_data_main()
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

# --- BƯỚC 3: FORM CẬP NHẬT ---
    elif st.session_state.step == 3:
        st.subheader("Bước 3: Cập nhật thông tin chi tiết")
        
        df, main_sheet, workbook = load_data_main()
        idx = st.session_state.selected_row_index
        
        try:
            current_data = df.loc[idx]
        except KeyError:
            st.error("Phiên làm việc đã hết hạn. Vui lòng tìm kiếm lại.")
            st.stop()

        with st.form("update_form"):
            updated_values = {}
            st.write("Kiểm tra và chỉnh sửa các thông tin dưới đây:")

            # Danh sách Optional
            OPTIONAL_COLS = [
                'Số thẻ Đảng* (12 số theo HD38-HD/BTCTW)',
                'Ngày cấp thẻ Đảng (dd/mm/yyyy)',
                'Số thẻ theo Đảng quyết định 85',
                'Ngày vào Đảng chính thức* (dd/mm/yyyy)',
                'Nơi cấp thẻ Đảng',
                'Số CMND cũ (nếu có)',
                'Tên gọi khác'
            ]

            for col in ALL_COLUMNS:
                if col in TEMP_COLS: continue
                
                # --- HEADER PHÂN VÙNG ---
                if col == 'Nơi đăng ký khai sinh - Quốc gia *':
                    st.markdown("---") 
                    st.subheader("🏠 THÔNG TIN KHAI SINH")
                elif col == 'Quê quán (theo mô hình 2 cấp) - Quốc gia *':
                    st.markdown("---")
                    st.subheader("🏠 THÔNG TIN QUÊ QUÁN")
                elif col == 'Thường trú (theo mô hình 2 cấp) - Quốc gia *':
                    st.markdown("---")
                    st.subheader("🏠 THÔNG TIN THƯỜNG TRÚ")

                val = current_data.get(col, "")
                
                # --- XỬ LÝ 1: NƠI ĐĂNG KÝ KHAI SINH (Tách chuỗi) ---
                if col == 'Nơi đăng ký khai sinh - Địa chỉ chi tiết *':
                    val_xa = current_data.get('Temp_XaPhuong_KhaiSinh', '')
                    val_thon = current_data.get('Temp_ThonTo_KhaiSinh', '')
                    
                    if not val_xa and not val_thon and str(val):
                        parts = str(val).split(',')
                        if len(parts) >= 2:
                            val_xa = parts[-1].strip()
                            val_thon = ",".join(parts[:-1]).strip()
                        else:
                            val_thon = str(val)

                    col1, col2 = st.columns(2)
                    with col1:
                        input_xa = st.text_input(
                            "Xã/Phường/ Đặc khu *", 
                            value=str(val_xa), placeholder="Ví dụ: Xã Văn Giang",
                            key="ks_xa"
                        )
                    with col2:
                        input_thon = st.text_input(
                            "Địa chỉ chi tiết dưới Phường/Xã (Thôn/Tổ...)*", 
                            value=str(val_thon), placeholder="Ví dụ: Thôn Hòa Bình Hạ",
                            key="ks_thon"
                        )
                    
                    st.caption("💡 Chú ý cách nhập địa chỉ chi tiết dưới Phường/Xã: ví dụ Thôn Hòa Bình Hạ/ Tổ dân số 5/ Số 60 Ngách 6/12 Đội Nhân")
                    
                    final_address = f"{input_thon}, {input_xa}".strip(", ")
                    updated_values[col] = final_address
                    updated_values['Temp_XaPhuong_KhaiSinh'] = input_xa
                    updated_values['Temp_ThonTo_KhaiSinh'] = input_thon

                # --- XỬ LÝ 2: THƯỜNG TRÚ (Tách chuỗi) ---
                elif col == 'Thường trú (theo mô hình 2 cấp) - Địa chỉ chi tiết *':
                    val_xa_tt = current_data.get('Temp_XaPhuong_ThuongTru', '')
                    val_thon_tt = current_data.get('Temp_ThonTo_ThuongTru', '')
                    
                    if not val_xa_tt and not val_thon_tt and str(val):
                        parts = str(val).split(',')
                        if len(parts) >= 2:
                            val_xa_tt = parts[-1].strip()
                            val_thon_tt = ",".join(parts[:-1]).strip()
                        else:
                            val_thon_tt = str(val)

                    col1, col2 = st.columns(2)
                    with col1:
                        input_xa_tt = st.text_input(
                            "Xã/Phường/ Đặc khu *", 
                            value=str(val_xa_tt), placeholder="Ví dụ: Phường Đồng Tâm",
                            key="tt_xa"
                        )
                    with col2:
                        input_thon_tt = st.text_input(
                            "Địa chỉ chi tiết dưới Phường/Xã (Thôn/Tổ...)*", 
                            value=str(val_thon_tt), placeholder="Ví dụ: Số 60 Ngách 6/12",
                            key="tt_thon"
                        )
                    
                    st.caption("💡 Chú ý cách nhập địa chỉ chi tiết dưới Phường/Xã: ví dụ Thôn Hòa Bình Hạ/ Tổ dân số 5/ Số 60 Ngách 6/12 Đội Nhân")

                    final_address_tt = f"{input_thon_tt}, {input_xa_tt}".strip(", ")
                    updated_values[col] = final_address_tt
                    updated_values['Temp_XaPhuong_ThuongTru'] = input_xa_tt
                    updated_values['Temp_ThonTo_ThuongTru'] = input_thon_tt

                # --- XỬ LÝ 3: QUÊ QUÁN (Chỉ hiển thị Xã) ---
                elif col == 'Quê quán (theo mô hình 2 cấp) - Địa chỉ chi tiết *':
                    updated_values[col] = st.text_input("Xã/Phường/ Đặc khu *", value=str(val), placeholder="Ví dụ: Xã Văn Giang", key="qq_xa")

                # --- CÁC TRƯỜNG CÒN LẠI ---
                else:
                    display_label = col
                    # Rút gọn tên hiển thị
                    if "Nơi đăng ký khai sinh" in col: display_label = col.replace("Nơi đăng ký khai sinh - ", "")
                    if "Quê quán (theo mô hình 2 cấp)" in col: display_label = col.replace("Quê quán (theo mô hình 2 cấp) - ", "")
                    if "Thường trú (theo mô hình 2 cấp)" in col: display_label = col.replace("Thường trú (theo mô hình 2 cấp) - ", "")

                    if col in OPTIONAL_COLS:
                        display_label = display_label.replace('*', '') + " (Không bắt buộc)"
                    
                    if col in READ_ONLY_COLS:
                        st.text_input(display_label, value=val, disabled=True, key=col)
                        updated_values[col] = str(val)
                    elif col == 'Trạng thái hoạt động':
                        opts = ["Đang sinh hoạt Đảng", "Đã chuyển sinh hoạt", "Đã từ trần", "Đã ra khỏi Đảng"]
                        idx_opt = opts.index(val) if val in opts else 0
                        updated_values[col] = st.selectbox(display_label, opts, index=idx_opt, key=col)
                    elif col == 'Giới tính *':
                        opts = ["Nam", "Nữ"]
                        idx_opt = opts.index(val) if val in opts else 0
                        updated_values[col] = st.selectbox(display_label, opts, index=idx_opt, key=col)
                    else:
                        ph = "Để trống nếu chưa có thông tin" if col in OPTIONAL_COLS else ""
                        updated_values[col] = st.text_input(display_label, value=str(val), placeholder=ph, key=col)

            st.write("---")
            submit_update = st.form_submit_button("💾 LƯU THÔNG TIN", type="primary")

            if submit_update:
                # --- VALIDATION (NÂNG CẤP: Check chi tiết Xã/Thôn) ---
                REQUIRE_COLUMNS = [
                    'STT', 'ID', 'Họ và tên *', 'Giới tính *', 'Sinh ngày * (dd/mm/yyyy)',
                    'Dân tộc *', 'Tôn giáo *', 'Số định danh cá nhân *',
                    'Nơi đăng ký khai sinh - Quốc gia *', 'Nơi đăng ký khai sinh - Tỉnh *', 
                    'Nơi đăng ký khai sinh - Địa chỉ chi tiết *', 
                    'Quê quán (theo mô hình 2 cấp) - Quốc gia *', 'Quê quán (theo mô hình 2 cấp) - Tỉnh *',
                    'Quê quán (theo mô hình 2 cấp) - Địa chỉ chi tiết *', 
                    'Thường trú (theo mô hình 2 cấp) - Quốc gia *', 'Thường trú (theo mô hình 2 cấp) - Tỉnh *', 
                    'Thường trú (theo mô hình 2 cấp) - Địa chỉ chi tiết *', 
                    'Ngày vào Đảng* (dd/mm/yyyy)', 'Trạng thái hoạt động'
                ]

                missing_fields = []
                for col_req in REQUIRE_COLUMNS:
                    # 1. Kiểm tra đặc biệt cho KHAI SINH (Check riêng Xã và Thôn)
                    if col_req == 'Nơi đăng ký khai sinh - Địa chỉ chi tiết *':
                        if not str(updated_values.get('Temp_XaPhuong_KhaiSinh', '')).strip():
                            missing_fields.append("Khai sinh: Xã/Phường/Đặc khu")
                        if not str(updated_values.get('Temp_ThonTo_KhaiSinh', '')).strip():
                            missing_fields.append("Khai sinh: Thôn/Tổ/Số nhà")
                        continue # Đã check xong cột này, bỏ qua check thường

                    # 2. Kiểm tra đặc biệt cho THƯỜNG TRÚ (Check riêng Xã và Thôn)
                    if col_req == 'Thường trú (theo mô hình 2 cấp) - Địa chỉ chi tiết *':
                        if not str(updated_values.get('Temp_XaPhuong_ThuongTru', '')).strip():
                            missing_fields.append("Thường trú: Xã/Phường/Đặc khu")
                        if not str(updated_values.get('Temp_ThonTo_ThuongTru', '')).strip():
                            missing_fields.append("Thường trú: Thôn/Tổ/Số nhà")
                        continue

                    # 3. Kiểm tra đặc biệt cho QUÊ QUÁN (Chỉ cần check giá trị chính vì chỉ nhập Xã)
                    if col_req == 'Quê quán (theo mô hình 2 cấp) - Địa chỉ chi tiết *':
                        if not str(updated_values.get(col_req, '')).strip():
                            missing_fields.append("Quê quán: Xã/Phường/Đặc khu")
                        continue

                    # 4. Kiểm tra thông thường cho các cột khác
                    val_check = str(updated_values.get(col_req, "")).strip()
                    if not val_check:
                        clean_name = col_req.replace('*', '')
                        clean_name = clean_name.replace("Nơi đăng ký khai sinh - ", "Khai sinh: ")
                        clean_name = clean_name.replace("Quê quán (theo mô hình 2 cấp) - ", "Quê quán: ")
                        clean_name = clean_name.replace("Thường trú (theo mô hình 2 cấp) - ", "Thường trú: ")
                        missing_fields.append(clean_name)
                
                if missing_fields:
                    st.error("⚠️ KHÔNG THỂ LƯU! Bạn chưa điền các thông tin bắt buộc sau:", icon="🚫")
                    for field in missing_fields:
                        st.markdown(f"- **{field}**")
                else:
                    with st.spinner("Đang lưu dữ liệu..."):
                        try:
                            row_vals = [updated_values.get(c, "") for c in ALL_COLUMNS]
                            try:
                                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                workbook.worksheet(SHEET_NAME_BACKUP).append_row([ts] + row_vals)
                            except: pass

                            sheet_row_number = idx + 2 
                            main_sheet.update(f"A{sheet_row_number}", [row_vals])
                            
                            st.session_state.step = 4
                            st.rerun()
     
                        except Exception as e:
                            st.error(f"Có lỗi hệ thống khi lưu: {e}")

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
    
    password = st.sidebar.text_input("Nhập mật khẩu Admin:", type="password")
    
    if password == ADMIN_PASSWORD:
        with st.spinner("Đang tải dữ liệu thống kê..."):
            # Load dữ liệu mới nhất từ Sheet1
            df_main, _, workbook = load_data_main()
            
            try:
                backup_sheet = workbook.worksheet(SHEET_NAME_BACKUP)
                backup_rows = backup_sheet.get_all_values()
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
            
            # Hiển thị trên web (Vẫn chỉ hiện ít cột cho gọn giao diện)
            display_cols = ['ID', 'Họ và tên *', 'Sinh ngày * (dd/mm/yyyy)', 'Tổ chức Đảng đang sinh hoạt * (không sửa)']
            st.dataframe(
                not_updated_df[display_cols],
                use_container_width=True,
                hide_index=True
            )

            # --- XỬ LÝ XUẤT FILE EXCEL ĐẦY ĐỦ ---
            # Tạo bộ nhớ đệm cho file Excel
            buffer_missing = io.BytesIO()
            
            # Ghi toàn bộ dữ liệu (not_updated_df) ra Excel, không lọc cột
            with pd.ExcelWriter(buffer_missing, engine='openpyxl') as writer:
                not_updated_df.to_excel(writer, index=False, sheet_name='ChuaCapNhat')
            
            # Đưa con trỏ về đầu file
            buffer_missing.seek(0)
            
            # Tên file kèm thời gian
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

            # --- PHẦN 2: TẢI FILE TỔNG HỢP (MỚI THÊM) ---
            st.subheader("🗄️ Xuất dữ liệu tổng hợp đầy đủ")
            st.write("Tải về file Excel chứa toàn bộ dữ liệu mới nhất từ hệ thống (bao gồm cả những người đã cập nhật và chưa cập nhật).")

            # Xử lý xuất file Excel trong bộ nhớ (RAM) mà không cần lưu ra ổ cứng
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_main.to_excel(writer, index=False, sheet_name='DanhSachTongHop')
            
            # Đưa con trỏ về đầu file để chuẩn bị tải
            buffer.seek(0)

            file_name_excel = f"TongHop_DangVien_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

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





























