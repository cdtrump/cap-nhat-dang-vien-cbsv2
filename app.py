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
    data = sheet.get_all_records(expected_headers=ALL_COLUMNS)
    df = pd.DataFrame(data)
    # Ép kiểu ID về string để so sánh
    df['ID'] = df['ID'].astype(str).replace(r'\.0$', '', regex=True)
    return df, sheet, workbook

# --- GIAO DIỆN CHÍNH ---
st.set_page_config(page_title="Hệ thống Quản lý Đảng viên", layout="wide")

# --- SIDEBAR MENU ---
st.sidebar.title("Menu")
app_mode = st.sidebar.radio("Chọn chức năng:", ["👤 Cập nhật thông tin", "📊 Admin Dashboard"])

# =========================================================
# CHẾ ĐỘ 1: NGƯỜI DÙNG CẬP NHẬT (Code cũ)
# =========================================================
if app_mode == "👤 Cập nhật thông tin":
    st.title("📝 Cập nhật thông tin Đảng viên")
    
    if 'step' not in st.session_state: st.session_state.step = 1
    if 'selected_row_index' not in st.session_state: st.session_state.selected_row_index = None

    # Bước 1: Tìm kiếm
    if st.session_state.step == 1:
        st.subheader("Tra cứu thông tin cá nhân")
        with st.form("search_form"):
            col_s1, col_s2 = st.columns(2)
            with col_s1: search_name = st.text_input("Họ và tên (đầy đủ có dấu):")
            with col_s2: search_dob = st.text_input("Ngày sinh (dd/mm/yyyy):", placeholder="05/01/2005")
            submitted = st.form_submit_button("Tra cứu")

            if submitted:
                if not search_name or not search_dob:
                    st.warning("Vui lòng nhập đầy đủ thông tin.")
                else:
                    df, _, _ = load_data_main()
                    mask = (df['Họ và tên *'].str.strip().str.lower() == search_name.strip().lower()) & \
                           (df['Sinh ngày * (dd/mm/yyyy)'] == search_dob.strip())
                    results = df[mask]
                    if results.empty:
                        st.error("❌ Không tìm thấy thông tin.")
                    else:
                        st.session_state.search_results = results
                        st.session_state.step = 2
                        st.rerun()

    # Bước 2: Chọn người
    elif st.session_state.step == 2:
        st.subheader("Xác nhận danh tính")
        results = st.session_state.search_results
        for index, row in results.iterrows():
            with st.container(border=True):
                c1, c2 = st.columns([4, 1])
                c1.markdown(f"**{row['Họ và tên *']}** - {row['Sinh ngày * (dd/mm/yyyy)']}")
                c1.text(f"Đơn vị: {row['Tổ chức Đảng đang sinh hoạt * (không sửa)']}")
                if c2.button("CẬP NHẬT", key=f"btn_{index}"):
                    st.session_state.selected_row_index = index
                    st.session_state.step = 3
                    st.rerun()
        if st.button("⬅️ Quay lại"):
            st.session_state.step = 1
            st.rerun()

    # Bước 3: Form cập nhật
    elif st.session_state.step == 3:
        st.subheader("Cập nhật thông tin chi tiết")
        df, main_sheet, workbook = load_data_main()
        idx = st.session_state.selected_row_index
        current_data = df.loc[idx]

        with st.form("update_form"):
            updated_values = {}
            for col in ALL_COLUMNS:
                val = current_data.get(col, "")
                if col in READ_ONLY_COLS:
                    st.text_input(col, value=val, disabled=True)
                    updated_values[col] = str(val)
                elif col == 'Trạng thái hoạt động':
                    opts = ["Đang sinh hoạt Đảng", "Đã chuyển sinh hoạt", "Đã từ trần", "Đã ra khỏi Đảng"]
                    updated_values[col] = st.selectbox(col, opts, index=opts.index(val) if val in opts else 0)
                elif col == 'Giới tính *':
                    opts = ["Nam", "Nữ"]
                    updated_values[col] = st.selectbox(col, opts, index=opts.index(val) if val in opts else 0)
                else:
                    updated_values[col] = st.text_input(col, value=str(val))
            
            if st.form_submit_button("💾 LƯU THÔNG TIN"):
                try:
                    row_vals = [updated_values[col] for col in ALL_COLUMNS]
                    # Ghi Backup
                    try:
                        workbook.worksheet(SHEET_NAME_BACKUP).append_row([datetime.now().strftime("%Y-%m-%d %H:%M:%S")] + row_vals)
                    except: pass 
                    # Ghi Main
                    main_sheet.update(f"A{idx + 2}", [row_vals])
                    st.success("✅ Cập nhật thành công!"); st.balloons()
                    st.session_state.step = 1
                    st.session_state.selected_row_index = None
                    st.rerun()
                except Exception as e: st.error(f"Lỗi: {e}")
        
        if st.button("Hủy"):
            st.session_state.step = 2
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