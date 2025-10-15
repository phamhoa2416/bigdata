import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
from typing import Any, Dict, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JobDataProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='jobs_topic'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.setup_producer()

    def setup_producer(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=5,
                max_block_ms=60000,
                compression_type='gzip',
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Kafka producer connected to {self.bootstrap_servers}")

            self.producer.send(self.topic, {'message': 'Producer initialized successfully'})
            self.producer.flush(timeout=10)
            logger.info("Kafka connection test successful.")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error setting up producer: {e}")
            raise

    def send_job_data(self, job_data: Dict[str, Any]) -> bool:
        try:
            key = job_data.get('Title', 'unknown')

            job_data['ingestion_time'] = time.time()

            future = self.producer.send(
                topic=self.topic,
                key=key,
                value=job_data
            )

            record_metadata = future.get(timeout=10)

            logger.info(
                f"Message sent to topic: {record_metadata.topic}, "
                f"partition: {record_metadata.partition}, "
                f"offset: {record_metadata.offset}"
            )

            return True
        except KafkaError as e:
            logger.error(f"Kafka error sending message: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending job data to Kafka: {e}")
            return False

    def send_batch_job_data(self, jobs_data: List[Dict[str, Any]]) -> Dict[str, int]:
        results = {
            'successful': 0,
            'failed': 0,
            'total': len(jobs_data)
        }

        for job_data in jobs_data:
            success = self.send_job_data(job_data)
            if success:
                results['successful'] += 1
                logging.info(f"Sent job to Kafka: {job_data['Title']}")
            else:
                results['failed'] += 1
                logging.error(f"Failed to send job: {job_data['Title']}")

            time.sleep(0.1)

        logging.info(
            f"Batch send completed: {results['successful']}/"
            f"{results['total']} successful, {results['failed']} failed"
        )
        return results

    def flush_messages(self):
        try:
            self.producer.flush(timeout=30)
            logger.info("All pending messages flushed to Kafka")
        except Exception as e:
            logger.error(f"Error flushing messages: {e}")

    def close(self):
        if self.producer:
            self.flush_messages()
            self.producer.close(timeout=10)
            logger.info("Kafka producer closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == "__main__":
    job_lists = [
        {
            "Title": "Nhân Viên Kinh Doanh Logistics (Tp Hồ Chí Minh, Hà Nội, Hải Phòng, Thanh Hóa - 1 Năm Kinh Nghiệm Trở Lên)",
            "Salary": "Thoả thuận",
            "Location": "Hồ Chí Minh & 3 nơi khác",
            "Experience": "1 năm",
            "Description": "Nhân Viên Kinh Doanh Logistics (Tp Hồ Chí Minh, Hà Nội, Hải Phòng, Thanh Hóa - 1 Năm Kinh Nghiệm Trở Lên)\nThoả thuận\nHồ Chí Minh & 3 nơi khác\n1 năm\nXem chi tiết\nỨng tuyển ngay\nMô tả công việc\nThực hiện chào bán các dịch vụ giao nhận vận tải quốc tế, cước vận chuyển.\nGiải quyết các vấn đề phát sinh trong quá trình giao nhận hàng.\nChi tiết công việc sẽ trao đổi trong quá trình phỏng vấn.\nYêu cầu ứng viên\nƯu tiên những ứng viên có kinh nghiệm tối thiểu 01 năm trong ngành logistics.\nTiếng Anh khá hoặc ngoại ngữ khác là một lợi thế.\nNhiệt tình, năng động, trung thực và không ngừng học hỏi.\nQuyền lợi\nLương cứng + hoa hồng.\nTham gia BHXH, BHYT, BHTN theo quy định.\nLương tháng 13 và các phúc lợi liên quan sẽ căn cứ vào kết quả kinh doanh của Công ty.\nSales Agency tại HCM sẽ có cơ hội tham gia các chương trình, hội chợ tại nước ngoài, gặp gỡ đối tác.\nHỗ trợ chi phí tham gia các chương trình nâng cao nghiệp vụ....\nVà các chính sách phúc lợi hấp dẫn khác...\nĐịa điểm làm việc\n- Hồ Chí Minh: 44 Nguyễn Văn Kỉnh, Phường Thạnh Mỹ Lợi, Thủ Đức\n- Hải Phòng: Tầng 06, Tòa nhà Hoàng Phát, Số 4 Lô 2A Đường Lê Hồng Phong, Phường Đông Khe, Ngô Quyền\n- Thanh Hoá: Tầng 5 Tòa nhà Dầu Khí, Số 38A Đại Lộ Lê Lợi, Phường Điện Biên, TP Thanh Hoá\n...và 1 địa điểm khác\nCÔNG TY CỔ PHẦN TỐC ĐỘ\n25-99 nhân viên\n44 Đường Nguyễn Văn Kỉnh, Khu Phố 14, Phường Cát Lái, Thành Phố Hồ Chí Minh, Việt Nam\nXem trang công ty"
        },
        {
            "Title": "Nhân Viên Kinh Doanh Logistics (Tp Hồ Chí Minh, Hà Nội, Hải Phòng, Thanh Hóa - 1 Năm Kinh Nghiệm Trở Lên)",
            "Salary": "Thoả thuận",
            "Location": "Hồ Chí Minh & 3 nơi khác",
            "Experience": "1 năm",
            "Description": "Nhân Viên Kinh Doanh Logistics (Tp Hồ Chí Minh, Hà Nội, Hải Phòng, Thanh Hóa - 1 Năm Kinh Nghiệm Trở Lên)\nThoả thuận\nHồ Chí Minh & 3 nơi khác\n1 năm\nXem chi tiết\nỨng tuyển ngay\nMô tả công việc\nThực hiện chào bán các dịch vụ giao nhận vận tải quốc tế, cước vận chuyển.\nGiải quyết các vấn đề phát sinh trong quá trình giao nhận hàng.\nChi tiết công việc sẽ trao đổi trong quá trình phỏng vấn.\nYêu cầu ứng viên\nƯu tiên những ứng viên có kinh nghiệm tối thiểu 01 năm trong ngành logistics.\nTiếng Anh khá hoặc ngoại ngữ khác là một lợi thế.\nNhiệt tình, năng động, trung thực và không ngừng học hỏi.\nQuyền lợi\nLương cứng + hoa hồng.\nTham gia BHXH, BHYT, BHTN theo quy định.\nLương tháng 13 và các phúc lợi liên quan sẽ căn cứ vào kết quả kinh doanh của Công ty.\nSales Agency tại HCM sẽ có cơ hội tham gia các chương trình, hội chợ tại nước ngoài, gặp gỡ đối tác.\nHỗ trợ chi phí tham gia các chương trình nâng cao nghiệp vụ....\nVà các chính sách phúc lợi hấp dẫn khác...\nĐịa điểm làm việc\n- Hồ Chí Minh: 44 Nguyễn Văn Kỉnh, Phường Thạnh Mỹ Lợi, Thủ Đức\n- Hải Phòng: Tầng 06, Tòa nhà Hoàng Phát, Số 4 Lô 2A Đường Lê Hồng Phong, Phường Đông Khe, Ngô Quyền\n- Thanh Hoá: Tầng 5 Tòa nhà Dầu Khí, Số 38A Đại Lộ Lê Lợi, Phường Điện Biên, TP Thanh Hoá\n...và 1 địa điểm khác\nCÔNG TY CỔ PHẦN TỐC ĐỘ\n25-99 nhân viên\n44 Đường Nguyễn Văn Kỉnh, Khu Phố 14, Phường Cát Lái, Thành Phố Hồ Chí Minh, Việt Nam\nXem trang công ty"
        },
        {
            "Title": "Nhân Viên Kinh Doanh/ Tư Vấn/ Telesales Nguồn Hàng Trung Quốc Shopee, Lazada, Tiktok - Không Yêu Cầu Tiếng Trung (Thu Nhập 8 Đến 40 Triệu)",
            "Salary": "10 - 40 triệu",
            "Location": "Hà Nội",
            "Experience": "Không yêu cầu",
            "Description": "Nhân Viên Kinh Doanh/ Tư Vấn/ Telesales Nguồn Hàng Trung Quốc Shopee, Lazada, Tiktok - Không Yêu Cầu Tiếng Trung (Thu Nhập 8 Đến 40 Triệu)\n10 - 40 triệu\nHà Nội\nKhông yêu cầu\nXem chi tiết\nỨng tuyển ngay\nMô tả công việc\n\n- Tìm kiếm khách hàng có nhu cầu nhập hàng tận xưởng bên Trung Quốc.\n\n- Lập kế hoạch và thực hiện kế hoạch phát triển khách hàng mới, duy trì khách hàng cũ.\n\n- Tư vấn các giải pháp liên quan đến dịch vụ order hộ, vận chuyển hàng hoá trên sàn TMĐT 1688, taobao, alibaba.\n\n- Xây dựng sự hiểu biết và mối quan hệ tốt với các khách hàng mục tiêu chính.\n\n- Tư vấn tìm nguồn hàng phong phú cho khách hàng từ các shoρ Trung Quốc\n\n- Lập báo giá cho khách hàng, giải đáp thắc mắc, hỗ trợ khách hàng trong quá trình thực hiện đơn hàng\n\n- Phối hợp cùng các bộ phận khác (Phòng mua hàng + Phòng chăm sóc KH) để đảm báo tiến trình đơn hàng được hoàn thiện\n\nYêu cầu ứng viên\n\n- Có Kinh nghiệm về sale, telesale, tư vấn trên 6 tháng (sẽ được đào tạo cụ thể khi làm việc).\n\n- Tinh thần chăm chỉ, chịu khó, cầu tiến, có mục đích định hướng nghề nghiệp rõ ràng.\n\n- Có tinh thần trách nhiệm cao trong công việc\n\n- Nhanh nhẹn, chịu được áp lực công việc.\n\n-  Có Latop/máy tính cá nhân\n\n- Không Yêu Cầu Tiếng Trung.\n\n- Độ tuổi từ 1990 đến 2002\n\nQuyền lợi\n\n- Thu nhập từ 10tr đến 40 triệu (LCB + Lương năng lực + Lương doanh số + Thưởng ngày ngày, tuần).\n\n- Được học hỏi kinh doanh từ các chủ shop lớn.\n\n- Được đóng BHXH theo quy định nhà nước\n\n- Nhân viên thử việc được đào tạo bài bản.\n\n- Các hoạt động team building, du lịch, workout: Công ty hỗ trợ 100%.\n\n- Môi trường làm việc trẻ trung, năng động, thân thiện\n\n- Chế độ nghỉ Lễ, Tết, phép năm, lương thưởng tháng 13\n\nĐịa điểm làm việc\n- Hà Nội: 106 Hoàng Quốc Việt, Cầu Giấy\nThời gian làm việc\nThứ 2 - Thứ 7 (từ 08:00 đến 17:30)\nThứ 7 chỉ làm từ 8h - 12h, nghỉ các buổi chiều thứ 7\nCÔNG TY TNHH THƯƠNG MẠI DỊCH VỤ LOGISTICS LÝ GIA\n100-499 nhân viên\nSố 7 ngõ 106 Hoàng Quốc Việt, Phường Nghĩa Tân, Quận Cầu Giấy, Thành phố Hà Nội, Việt Nam\nXem trang công ty"
        },
        {
            "Title": "Nhân Viên Kinh Doanh",
            "Salary": "Thoả thuận",
            "Location": "Hải Phòng & 7 nơi khác",
            "Experience": "1 năm",
            "Description": "Nhân Viên Kinh Doanh\nThoả thuận\nHải Phòng & 7 nơi khác\n1 năm\nXem chi tiết\nỨng tuyển ngay\nMô tả công việc\nTìm kiếm, mở rộng, tư vấn và ký kết hợp đồng với khách hàng sử dụng dịch vụ forwarding/logistics quốc tế và nội địa theo thế mạnh công ty \nXây dựng và triển khai các kế hoạch kinh doanh đạt được mục tiêu cá nhân cũng như  mục tiêu chung của bộ phận từng giai đoạn\nChăm sóc duy trì mối quan hệ hợp tác lâu dài, tốt đẹp với khách hàng hiện có\nPhối hợp với các phòng ban nghiệp vụ để đảm bảo chất lượng dịch vụ tốt nhất, đem lại  sự hài lòng cho khách hàng \nThực hiện chế độ báo cáo theo yêu cầu của Công ty\n \nYêu cầu ứng viên\nTốt nghiệp Đại học các chuyên ngành Kinh tế đối ngoại, Ngoại thương, Logistic, Vận tải,... và các chuyên ngành liên quan; Có kiến thức vững chắc về ngành Logistics, Xuất nhập khẩu\nCó từ 1 năm kinh nghiệm về sales hàng consol xuất khẩu;\nNắm bắt thị trường consol và có mối quan hệ rộng với các master consol trên thị trường;\nCó kỹ năng giao tiếp, thuyết phục, đàm phán tốt;\nCó kỹ năng leadership, khả năng quản lý là một lợi thế;\nYêu thích và đam mê kinh doanh và giao tiếp nhiều với khách hàng;\nNhanh nhạy, trung thực, có tinh thần trách nhiệm và tinh thần tập thể cao.\n \nQuyền lợi\nLương cứng: thỏa thuận + thưởng hoa hồng lên đến 40% lợi nhuận (tổng thu  nhập không giới hạn) \nHưởng đầy đủ các chế độ theo Luật quy định (BHXH, BHYT, BHTN, nghỉ phép, nghỉ  lễ tết...). \nChế độ đãi ngộ tốt: định kỳ 6 tháng tăng lương 1 lần, lương tháng 13, thưởng lễ tết, du  lịch trong và ngoài nước, hỗ trợ ăn trưa, công tác phí… \nMôi trường làm việc chuyên nghiệp, nhiều cơ hội phát triển, đồng nghiệp vui vẻ thân  thiện \nTham gia các khóa đào tạo nâng cao trình độ chuyên môn, nghiệp vụ và tham quan thực  tế tại các cảng sân bay, kho bãi \n \nĐịa điểm làm việc\n- Hải Phòng: Tòa nhà 274, Đường Đà Nẵng, Phường Ngô Quyền, Ngô Quyền\n- Lạng Sơn: Số 309 Đường Hùng Vương, Xã Mai Pha, TP Lạng Sơn\n- Hà Nội: Epic Tower, 19 Duy Tân, Cầu Giấy, Cầu Giấy\n...và 5 địa điểm khác\nThời gian làm việc\nThứ 2 - Thứ 6 (từ 08:00 đến 17:30)\nThứ 7 (từ 08:00 đến 12:00)\nCÔNG TY CỔ PHẦN DỊCH VỤ HÀNG HẢI HÀNG KHÔNG CON CÁ HEO\n100-499 nhân viên\n39B Trường Sơn, Phường 4, Quận Tân Bình, TP Hồ Chí Minh.\nXem trang công ty"
        },
        {
            "Title": "Nhân Viên Sales Logistics - Yêu Cầu 1 Năm Kinh Nghiệm - Nam Từ Liêm - Hà Nội",
            "Salary": "20 - 40 triệu",
            "Location": "Hà Nội",
            "Experience": "1 năm",
            "Description": "Nhân Viên Sales Logistics - Yêu Cầu 1 Năm Kinh Nghiệm - Nam Từ Liêm - Hà Nội\n20 - 40 triệu\nHà Nội\n1 năm\nXem chi tiết\nỨng tuyển ngay\nMô tả công việc\n\n-Tư vấn dịch vụ vận tải hàng không hoặc đường biển, vận tải đường bộ, kho bãi và các dịch vụ hậu cần hỗ trợ khác.\n\n-Kiểm tra tỷ giá với các hãng tàu, hãng hàng không, nhà cung cấp dịch vụ vận tải đường bộ, đại lý nước ngoài.\n\n-Theo dõi , giám sát tiến độ của các booking \n\n\n\n\nYêu cầu ứng viên\n\n- Có kinh nghiệm > 1 năm ở vị trí tương đương, trong lĩnh vực Logistics (Sea/Air )\n\n- Có khả năng sử dụng tiếng Anh hoặc tiếng Trung cơ bản\n\n- Tốt nghiệp ĐH, CĐ chuyên ngành liên quan.\n\n- Có khả năng sử dụng tiếng Anh cơ bản \n\nQuyền lợi\n\n- Mức lương: Lương cứng : 10tr-15tr + thưởng doanh số theo hiệu quả kinh doanh (mức lương trung bình 20-40 triệu/tháng với các nhân sự làm việc tốt)\n\n- Các chế độ phúc lợi lao động theo quy định pháp luật, thưởng quý, lương tháng thứ 13, tăng lương 1 năm/lần, du lịch hàng năm, \n\n- Đóng bảo hiểm luôn sau 2 tháng thử việc\n\n-Môi trường làm việc năng động, trẻ trung, thân thiện, có cơ hội thăng tiến cao\n\nĐịa điểm làm việc\n- Hà Nội: Tầng 12 tháp A , Tòa nhà Sông Đà, Phạm Hùng, Phường Mỹ Đình 1, Nam Từ Liêm\nThời gian làm việc\nThứ 2 - Thứ 6 (từ 08:15 đến 17:15)\nThứ 7 (từ 08:15 đến 12:00)\nNghỉ trưa: 12h -13h30\n1 tháng làm 2 buổi sáng Thứ 7\nCHI NHÁNH CÔNG TY TNHH GIAO NHẬN GẤU TRÚC TOÀN CẦU\n25-99 nhân viên\ntòa nhà Sông Đà, đường Phạm Hùng, P. Mỹ Đình 1, Q. Nam Từ Liêm, Hà Nội\nXem trang công ty"
        },
        {
            "Title": "Nhân Viên Kinh Doanh/ Telesales/ Sales Logistics - Hà Nội, TP.HCM - Thu Nhập Up To 30 Triệu ++",
            "Salary": "12 - 30 triệu",
            "Location": "Hồ Chí Minh, Hà Nội",
            "Experience": "1 năm",
            "Description": "Nhân Viên Kinh Doanh/ Telesales/ Sales Logistics - Hà Nội, TP.HCM - Thu Nhập Up To 30 Triệu ++\n12 - 30 triệu\nHồ Chí Minh, Hà Nội\n1 năm\nXem chi tiết\nỨng tuyển ngay\nMô tả công việc\n\n- Tìm kiếm khách hàng có nhu cầu sử dụng dịch vụ Xuất/ Nhập khẩu\n\n- Phân tích, đánh giá, đàm phán, thuyết phục khách hàng sử dụng dịch vụ vận chuyển.\n\n- Lập kế hoạch tiếp xúc và triển khai tiếp xúc, đi thị trường, phát triển khách hàng.\n\n- Duy trì các mối quan hệ với khách hàng cũ và phát triển các mối quan hệ mới.\n\n- Xây dựng, thực hiện, báo cáo kế hoạch và kết quả làm việc theo ngày, tuần, tháng\n\nYêu cầu ứng viên\n\n- Tốt nghiệp ĐH, CĐ, ưu tiên ứng viên tốt nghiệp các chuyên ngành liên quan đến Logistics, Kinh tế quốc tế, Xuất nhập khẩu, Thương mại quốc tế, Ngoại ngữ,...\n\n- Đã có kinh nghiệm Sales Logistics hoặc có hiểu biết về Logistics và nghiệp vụ xuất nhập khẩu\n\n- Độ tuổi: 22 - 38\n\n- Nhanh nhẹn, giao tiếp tốt.\n\n- Có khả năng giao tiếp tốt Tiếng Anh/ Tiếng Trung/ Tiếng Nhật là một lợi thế\n\nQuyền lợi\n\nLương cơ bản: 8 - 12 triệu\n\nThu nhập trung bình 12 - 30 triệu/tháng (thưởng KPIs không giới hạn)\n\n- Xét tăng lương 2 lần/năm. Mỗi lần duyệt tăng lương: 800.000đ/ tháng.\n\n- Thưởng lương tháng 13 ( trung bình 2 tháng lương) + quà Tết cho gia đình ( TV: 500k; dưới 6 tháng: 2 tr; dưới 1 năm: 3 triệu; 1 - 3 năm: 4 triệu ........). Thưởng ngành/ phòng/ lao động xuất sắc/ lao động tốt....\n\n- Thưởng nóng, thưởng doanh thu, doanh số, theo tuần/tháng/quý, thưởng cuối năm theo cá nhân, phòng/team\n\n- Thưởng các ngày Lễ, Tết, sinh nhật, trợ cấp,...\n\n- Chế độ BHXH đầy đủ theo quy định của Nhà nước\n\n- Đi du lịch trong ngoài nước 2 lần /năm, teambuilding,...\n\n- Làm việc trong môi trường trẻ chuyên nghiệp, năng động có cơ hội thăng tiến lên cấp quản lý.\n\nĐịa điểm làm việc\n- Hồ Chí Minh: Phường 12, Tân Bình\n- Hà Nội: CT4, Vimeco 2, Nguyễn Chánh, Yên Hòa, Cầu Giấy\nThời gian làm việc\nThứ 2 - Thứ 6 (từ 08:00 đến 17:00)\nThứ 7 làm việc online tại nhà\nCông ty cổ phần Hợp Nhất Quốc tế\n500-1000 nhân viên\nTầng 2, Tòa nhà CT4, Vimeco 2, phố Nguyễn Chánh, phường Trung Hòa, quận Cầu Giấy, thành phố Hà Nội\nXem trang công ty"
        }
    ]

    producer = JobDataProducer(
        bootstrap_servers='localhost:9092',
        topic='jobs_topic'
    )

    try:
        producer.send_batch_job_data(job_lists)
    finally:
        producer.close()
