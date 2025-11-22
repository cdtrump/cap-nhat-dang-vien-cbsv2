import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import io

# --- CẤU HÌNH ---
ADMIN_PASSWORD = st.secrets["admin_password"]


ALL_COLUMNS = [
    'STT', 'ID', 'Họ và tên *', 'Tên gọi khác', 'Giới tính *', 'Sinh ngày * (dd/mm/yyyy)',
    'Dân tộc *', 'Tôn giáo *', 'Số định danh cá nhân *', 'Số thẻ Đảng* (12 số theo HD38-HD/BTCTW)',
    'Nơi cấp thẻ Đảng', 'Ngày cấp thẻ Đảng (dd/mm/yyyy)', 'Số thẻ theo Đảng quyết định 85',
    'Tổ chức Đảng đang sinh hoạt * (không sửa)', 'Nơi đăng ký khai sinh - Quốc gia *',
    'Nơi đăng ký khai sinh - Tỉnh *', 'Nơi đăng ký khai sinh - Địa chỉ chi tiết *',
    'Quê quán (theo mô hình 2 cấp) - Quốc gia *', 'Quê quán (theo mô hình 2 cấp) - Tỉnh *',
    'Quê quán (theo mô hình 2 cấp) - Địa chỉ chi tiết *', 'Thường trú (theo mô hình 2 cấp) - Quốc gia *',
    'Thường trú (theo mô hình 2 cấp) - Tỉnh *', 'Thường trú (theo mô hình 2 cấp) - Địa chỉ chi tiết *',
    'Ngày vào Đảng* (dd/mm/yyyy)', 'Ngày vào Đảng chính thức* (dd/mm/yyyy)', 'Số CMND cũ (nếu có)',
    'Trạng thái hoạt động', 'Ngày rời khỏi/ Ngày mất/ Ngày miễn sinh hoạt Đảng (dd/mm/yyyy)'
]

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
st.set_page_config(page_title="Hệ thống Quản lý Đảng viên", layout="wide")

# --- SIDEBAR MENU ---
st.sidebar.title("Menu")
app_mode = st.sidebar.radio("Chọn chức năng:", ["👤 Cập nhật thông tin", "📊 Admin Dashboard"])

# =========================================================
# CHẾ ĐỘ 1: NGƯỜI DÙNG CẬP NHẬT
# =========================================================
if app_mode == "👤 Cập nhật thông tin":
    st.title("📝 Cập nhật thông tin Đảng viên")
    
    # Khởi tạo state nếu chưa có
    if 'step' not in st.session_state:
        st.session_state.step = 1
    if 'selected_row_index' not in st.session_state:
        st.session_state.selected_row_index = None

    # --- BƯỚC 1: TÌM KIẾM ---
    if st.session_state.step == 1:
        st.subheader("Bước 1: Tra cứu thông tin cá nhân")
        with st.form("search_form"):
            col_s1, col_s2 = st.columns(2)
            with col_s1:
                search_name = st.text_input("Họ và tên (đầy đủ có dấu):")
            with col_s2:
                search_dob = st.text_input("Ngày sinh (dd/mm/yyyy):", placeholder="Ví dụ: 05/01/2005")
            
            submitted = st.form_submit_button("Tra cứu", type="primary")

            if submitted:
                if not search_name or not search_dob:
                    st.warning("Vui lòng nhập đầy đủ Họ tên và Ngày sinh.")
                else:
                    with st.spinner("Đang tìm kiếm..."):
                        df, _, _ = load_data_main()
                        # Lọc dữ liệu (Case insensitive)
                        mask = (
                            df['Họ và tên *'].str.strip().str.lower() == search_name.strip().lower()
                        ) & (
                            df['Sinh ngày * (dd/mm/yyyy)'] == search_dob.strip()
                        )
                        results = df[mask]

                        if results.empty:
                            st.error("❌ Không tìm thấy thông tin hoặc bạn không thuộc diện cần cập nhật.")
                            st.info("Lưu ý: Kiểm tra kỹ chính tả và định dạng ngày sinh (dd/mm/yyyy).")
                        else:
                            st.success(f"Tìm thấy {len(results)} kết quả.")
                            st.session_state.search_results = results
                            st.session_state.step = 2
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
        
        # Load lại data mới nhất để đảm bảo tính toàn vẹn
        df, main_sheet, workbook = load_data_main()
        idx = st.session_state.selected_row_index
        
        try:
            current_data = df.loc[idx]
        except KeyError:
            st.error("Phiên làm việc đã hết hạn hoặc dữ liệu thay đổi. Vui lòng tìm kiếm lại.")
            if st.button("Quay về trang chủ"):
                st.session_state.step = 1
                st.rerun()
            st.stop()

        with st.form("update_form"):
            updated_values = {}
            
            st.write("kiểm tra và chỉnh sửa các thông tin dưới đây (nếu sai):")
                 
            for col in ALL_COLUMNS:
                val = current_data.get(col, "")
                
                # --- TRƯỜNG HỢP CHỈ ĐỌC ---
                if col in READ_ONLY_COLS:
                    st.text_input(col, value=val, disabled=True)
                    updated_values[col] = str(val)
                
                # --- TRƯỜNG HỢP DROPBOX ---
                elif col == 'Trạng thái hoạt động':
                    options = ["Đang sinh hoạt Đảng", "Đã chuyển sinh hoạt"]
                    try: opt_idx = options.index(val)
                    except: opt_idx = 0
                    updated_values[col] = st.selectbox(col, options, index=opt_idx)
                
                elif col == 'Giới tính *':
                    options = ["Nam", "Nữ"]
                    try: opt_idx = options.index(val)
                    except: opt_idx = 0
                    updated_values[col] = st.selectbox(col, options, index=opt_idx)

                # --- TRƯỜNG HỢP ĐỊA CHỈ (CÓ GỢI Ý) ---
                elif "Địa chỉ chi tiết" in col:
                    # Hiển thị label
                    st.markdown(f"{col}") 
                    
                    # Ô nhập liệu
                    updated_values[col] = st.text_input(
                        col, 
                        value=str(val), 
                        label_visibility="collapsed", # Ẩn label mặc định để dùng markdown phía trên cho đẹp
                        placeholder="Ví dụ: Thôn Hòa Bình Hạ, Xã Văn Giang, Tỉnh Hưng Yên"
                    )
                    # Dòng gợi ý màu xám bên dưới
                    st.caption("💡 *Định dạng mẫu: Thôn/Xóm/Số nhà/Tổ, Xã/Phường*")
                
                # --- CÁC TRƯỜNG KHÁC ---
                else:
                    updated_values[col] = st.text_input(col, value=str(val))

            st.write("---")
            submit_update = st.form_submit_button("💾 LƯU THÔNG TIN", type="primary")

            if submit_update:
                with st.spinner("Đang lưu dữ liệu lên hệ thống..."):
                    try:
                        # 1. Chuẩn bị dữ liệu
                        row_vals = [updated_values[col] for col in ALL_COLUMNS]
                        
                        # 2. Ghi vào Sheet BACKUP (Thử ghi, nếu lỗi thì bỏ qua để ko chặn user)
                        try:
                            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            backup_sheet = workbook.worksheet(SHEET_NAME_BACKUP)
                            backup_sheet.append_row([timestamp] + row_vals)
                        except Exception as e_backup:
                            print(f"Lỗi backup: {e_backup}") # Log lỗi ngầm

                        # 3. Cập nhật vào Sheet CHÍNH
                        # Index pandas bắt đầu từ 0, header sheet chiếm 1 dòng -> row thực tế = index + 2
                        sheet_row_number = idx + 2 
                        main_sheet.update(f"A{sheet_row_number}", [row_vals])
                        
                        # === CHUYỂN HƯỚNG SANG BƯỚC 4 (THÀNH CÔNG) ===
                        st.session_state.step = 4
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Có lỗi xảy ra khi lưu: {e}")

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
            not_updated_df = df_main[~df_main['ID'].isin(updated_ids)].copy()
            display_cols = ['ID', 'Họ và tên *', 'Sinh ngày * (dd/mm/yyyy)', 'Tổ chức Đảng đang sinh hoạt * (không sửa)']
            st.dataframe(not_updated_df[display_cols], use_container_width=True, hide_index=True)

            csv = not_updated_df[display_cols].to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 Tải danh sách CHƯA cập nhật (CSV)",
                data=csv,
                file_name='danh_sach_chua_cap_nhat.csv',
                mime='text/csv',
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


