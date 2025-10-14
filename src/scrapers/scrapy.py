from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.webelement import WebElement
import pandas as pd
import time, os, json
import re

# ========================
# Cấu hình người dùng
# ========================
cr_cb = [
    ('r1', 'b4'), ('r1', 'b5'), ('r1', 'b6'), ('r1', 'b7'),
    ('r1', 'b8'), ('r1', 'b9'), ('r1', 'b10'), ('r1', 'b11'),
    ('r1', 'b12'), ('r1', 'b13'), ('r1', 'b14'), ('r1', 'b15'),
    ('r1', 'b1081'), ('r1', 'b2'), ('r1', 'b3'), ('r1', 'b17'),
    ('r1', 'b18')
]

cb = ["r92", "r158", "r177", "r257", "r1042", "r206", "r333", "r1080", "r392",
      "r417", "r477", "r544", "r612", "r644", "r711", "r750", "r826", "r857",
      "r883", "r1010", "r1013", "r1014", "r899"]

URL_TEMPLATE = (
    "https://www.topcv.vn/tim-viec-lam-sales-xuat-nhap-khau-logistics-"
    "c{r}c{b}?type_keyword=1&page={page_number}&category_family={r}~{b}&sba=1"
)

SCROLL_END = 13000
SCROLL_STEP = 20
CLICK_INTERVAL = 0.8
CLICK_POSITION = (620, 337)
FIRST_CLICK_POSITION = (300, 300)


def save_to_excel(new_data, file_name):
    new_df = pd.DataFrame(new_data)

    if os.path.exists(file_name):
        # Đọc dữ liệu cũ
        old_df = pd.read_excel(file_name)

        # Gộp dữ liệu cũ + mới
        combined_df = pd.concat([old_df, new_df], ignore_index=True)

        # Xóa các dòng trùng (nếu cần, dựa theo cột url)
        combined_df.drop_duplicates(subset=["url"], inplace=True, ignore_index=True)
    else:
        combined_df = new_df

    # Ghi đè lại file Excel
    combined_df.to_excel(file_name, index=False)
    print(f"💾 Đã ghi {len(new_df)} dòng mới, tổng cộng {len(combined_df)} dòng trong {file_name}")


def save_to_json(new_data, filename="data.json"):
    # Nếu file đã tồn tại thì đọc dữ liệu cũ
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            try:
                existing_data = json.load(f)
                if not isinstance(existing_data, list):
                    existing_data = [existing_data]
            except json.JSONDecodeError:
                existing_data = []
    else:
        existing_data = []

    # Thêm dữ liệu mới vào
    if isinstance(new_data, list):
        existing_data.extend(new_data)
    else:
        existing_data.append(new_data)

    # Ghi lại toàn bộ dữ liệu ra file
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=4)

    print(f"✅ Đã lưu {len(new_data)} mục vào {filename}")


# ========================
# Cấu hình trình duyệt
# ========================
options = Options()
options.add_argument("--start-maximized")
driver = webdriver.Chrome(options=options)
driver.maximize_window()

# ========================
# Biến lưu dữ liệu toàn bộ
# ========================
all_data = []


# ========================
# Hàm scroll + click + crawl
# ========================
def scroll_and_crawl(driver, actions):
    current_scroll = 0
    last_click_time = time.time()
    while current_scroll < SCROLL_END:
        driver.execute_script(f"window.scrollTo(0, {current_scroll});")
        time.sleep(0.05)
        current_scroll += SCROLL_STEP

        if time.time() - last_click_time >= CLICK_INTERVAL:
            # Click mô phỏng
            actions.move_by_offset(CLICK_POSITION[0], CLICK_POSITION[1]).click().perform()
            actions.move_by_offset(-CLICK_POSITION[0], -CLICK_POSITION[1])
            time.sleep(2)
            last_click_time = time.time()

            try:
                job_detail = driver.find_element(By.CSS_SELECTOR, "div.box-job-info")
                job_header_detail = driver.find_element(By.CSS_SELECTOR, "div.box-header")
                headers = job_header_detail.find_elements(By.CSS_SELECTOR, "div.box-item-header")
                title = job_header_detail.find_element(By.TAG_NAME, "h2").text.strip()

                salary, location, experience = "", "", ""
                for header in headers:
                    if salary == "":
                        salary = header.text.strip()
                    elif location == "":
                        location = header.text.strip()
                    elif experience == "":
                        experience = header.text.strip()

                description = job_detail.get_attribute("innerText").strip()
                all_data.append({
                    "Title": title,
                    "Salary": salary,
                    "Location": location,
                    "Experience": experience,
                    "Description": description
                })
                print(salary, location, experience, description)

            except Exception as e:
                print("⚠️ Không lấy được box-view-job-detail:", e)
                continue
    save_to_json(all_data, "crawl_job_details_full.json")
    all_data.clear()  # Xóa dữ liệu sau khi lưu


# ========================
# Chạy vòng lặp chính
# ========================


for r, b in cr_cb:
    page_number = 1
    url = URL_TEMPLATE.format(r=r, b=b, page_number=page_number)
    print(f"\n🚀 Đang xử lý cặp ({r}, {b}) - Trang đầu: {url}")
    driver.get(url)
    time.sleep(2)

    # ------------------------
    # Lấy số trang tối đa
    # ------------------------
    try:
        span_text = driver.find_element(By.CSS_SELECTOR, "span#job-listing-paginate-text").get_attribute("innerHTML")
        # Dạng: "1 /&nbsp; 5" -> lấy số sau dấu /
        match = re.search(r'/&nbsp;\s*(\d+)', span_text)
        max_page = int(match.group(1)) if match else 200
        print(f"🔢 Số trang tối đa: {max_page}")
    except Exception:
        max_page = 200
        print("⚠️ Không tìm thấy số trang, mặc định 200")

    actions = ActionChains(driver)

    # ------------------------
    # Duyệt qua từng trang
    # ------------------------
    for page_number in range(1, max_page + 1):
        try:
            url = URL_TEMPLATE.format(r=r, b=b, page_number=page_number)
            print(f"\n➡️ Đang vào trang {page_number}/{max_page} ({r},{b})")
            driver.get(url)
            time.sleep(2)
            scroll_and_crawl(driver, actions)
        except Exception as e:
            print(f"❌ Lỗi khi xử lý trang {page_number}: {e}")
            continue

# ========================
# Sau khi hoàn tất
# ========================
driver.quit()