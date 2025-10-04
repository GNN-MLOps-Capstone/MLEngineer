import psycopg2
from newspaper import Article
import time
from datetime import datetime
from urllib.parse import urlparse
import logging
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NewsCrawler:
    def __init__(self, filter_file_path, crawler_version='1.0.0', max_workers=5):
        self.filter_file_path = filter_file_path
        self.crawler_version = crawler_version
        self.filter_version = self._extract_filter_version(filter_file_path)
        self.filtered_domains = self._load_filter_domains()
        self.max_workers = max_workers
        self.db_lock = Lock()  # 데이터베이스 작업용 락

    def _extract_filter_version(self, file_path):
        """파일명에서 버전 정보 추출"""
        try:
            version = file_path.split('_v')[-1].replace('.txt', '')
            return f'v{version}'
        except:
            return 'v1.00'

    def _load_filter_domains(self):
        """필터링할 도메인 리스트 로드"""
        try:
            with open(self.filter_file_path, 'r', encoding='utf-8') as f:
                domains = {line.strip().replace('www.', '') for line in f if line.strip()}
            logger.info(f"필터 도메인 {len(domains)}개 로드 완료 (버전: {self.filter_version})")
            return domains
        except Exception as e:
            logger.error(f"필터 파일 로드 중 오류: {e}")
            return set()

    def _is_url_filtered(self, url):
        """URL이 필터링 대상인지 확인"""
        try:
            domain = urlparse(url).netloc.replace('www.', '')
            return any(fd in domain or domain in fd for fd in self.filtered_domains)
        except:
            return False

    def _get_db_connection(self):
        """데이터베이스 연결 생성"""
        return psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )

    def filter_urls(self):
        """
        모든 pending 상태의 뉴스 URL을 필터링하여 상태 업데이트
        """
        conn = self._get_db_connection()
        cursor = conn.cursor()

        try:
            # 모든 pending 상태의 뉴스 가져오기
            cursor.execute("""
                SELECT news_id, url 
                FROM naver_news 
                WHERE crawl_status = 'pending'
                ORDER BY news_id DESC
            """)

            news_list = cursor.fetchall()

            if not news_list:
                logger.info("필터링할 pending 데이터가 없습니다.")
                return 0

            logger.info(f"URL 필터링 시작: {len(news_list):,}개")

            # 필터링 결과 분류
            filtered_ids = []
            passed_ids = []

            for news_id, url in news_list:
                if self._is_url_filtered(url):
                    filtered_ids.append(news_id)
                else:
                    passed_ids.append(news_id)

            # 배치 업데이트 (url_filtered)
            if filtered_ids:
                cursor.execute("""
                    UPDATE naver_news 
                    SET crawl_status = 'url_filtered',
                        url_filter_version = %s
                    WHERE news_id = ANY(%s)
                """, (self.filter_version, filtered_ids))

            # 배치 업데이트 (pending 유지 - 크롤링 대상)
            if passed_ids:
                cursor.execute("""
                    UPDATE naver_news 
                    SET url_filter_version = %s
                    WHERE news_id = ANY(%s)
                """, (self.filter_version, passed_ids))

            conn.commit()
            logger.info(f"URL 필터링 완료 - 필터링: {len(filtered_ids):,}개, 통과: {len(passed_ids):,}개")

            return len(filtered_ids)

        except Exception as e:
            conn.rollback()
            logger.error(f"URL 필터링 중 오류: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def crawl_article(self, url):
        """newspaper3k를 사용하여 기사 크롤링"""
        start_time = time.time()

        try:
            article = Article(url, language='ko')
            article.download()
            article.parse()

            response_time_ms = int((time.time() - start_time) * 1000)
            text = article.text.strip()

            return text if text else None, response_time_ms

        except Exception as e:
            response_time_ms = int((time.time() - start_time) * 1000)
            logger.debug(f"크롤링 실패 ({url}): {e}")
            return None, response_time_ms

    def _process_single_news(self, news_data, idx, total):
        """단일 뉴스 크롤링 처리 (스레드에서 실행)"""
        news_id, url, title = news_data

        # 크롤링 진행 중 상태로 변경
        conn = self._get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                UPDATE naver_news 
                SET crawl_status = 'crawling',
                    crawl_attempt_count = crawl_attempt_count + 1
                WHERE news_id = %s
            """, (news_id,))
            conn.commit()

            logger.info(f"[{idx}/{total}] {title[:50]}...")

            # 기사 크롤링
            text, response_time_ms = self.crawl_article(url)

            if text:
                # 크롤링 성공
                try:
                    cursor.execute("""
                        INSERT INTO crawled_news 
                        (news_id, text, crawler_version, response_time_ms, crawled_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (news_id, text, self.crawler_version, response_time_ms, datetime.now()))

                    cursor.execute("""
                        UPDATE naver_news 
                        SET crawl_status = 'crawl_success'
                        WHERE news_id = %s
                    """, (news_id,))

                    conn.commit()
                    logger.info(f"  ✓ 성공 ({len(text):,}자, {response_time_ms}ms)")
                    return 'success'

                except psycopg2.IntegrityError:
                    conn.rollback()
                    cursor.execute("""
                        UPDATE naver_news 
                        SET crawl_status = 'crawl_success'
                        WHERE news_id = %s
                    """, (news_id,))
                    conn.commit()
                    logger.warning(f"  ⚠ 이미 크롤링됨")
                    return 'success'

            else:
                # 크롤링 실패 (본문 없음)
                cursor.execute("""
                    UPDATE naver_news 
                    SET crawl_status = 'crawl_failed'
                    WHERE news_id = %s
                """, (news_id,))
                conn.commit()
                logger.warning(f"  ✗ 실패: 본문 없음")
                return 'failed'

        except Exception as e:
            conn.rollback()
            logger.error(f"  ✗ 오류: {e}")
            return 'failed'
        finally:
            cursor.close()
            conn.close()

    def crawl_news(self):
        """
        모든 pending 상태의 뉴스를 멀티스레드로 크롤링
        """
        conn = self._get_db_connection()
        cursor = conn.cursor()

        try:
            # pending 상태이면서 url_filter_version이 설정된 모든 뉴스 가져오기
            cursor.execute("""
                SELECT news_id, url, title
                FROM naver_news 
                WHERE crawl_status = 'pending'
                  AND url_filter_version IS NOT NULL
                ORDER BY pub_date DESC
            """)

            news_list = cursor.fetchall()

            if not news_list:
                logger.info("크롤링할 데이터가 없습니다.")
                return 0, 0

            total = len(news_list)
            logger.info(f"크롤링 시작: {total:,}개 (스레드: {self.max_workers}개)")

            success_count = 0
            failed_count = 0

            # ThreadPoolExecutor를 사용한 병렬 처리
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 모든 작업을 제출
                future_to_news = {
                    executor.submit(self._process_single_news, news_data, idx, total): news_data
                    for idx, news_data in enumerate(news_list, 1)
                }

                # 완료된 작업 처리
                for future in as_completed(future_to_news):
                    result = future.result()
                    if result == 'success':
                        success_count += 1
                    else:
                        failed_count += 1

            logger.info(f"크롤링 완료 - 성공: {success_count:,}개, 실패: {failed_count:,}개")

            return success_count, failed_count

        except Exception as e:
            logger.error(f"크롤링 중 오류: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def run(self):
        """전체 크롤링 프로세스 실행"""
        start_time = time.time()

        logger.info("=" * 60)
        logger.info(f"뉴스 크롤링 시작")
        logger.info(f"크롤러 버전: {self.crawler_version}")
        logger.info(f"필터 버전: {self.filter_version}")
        logger.info(f"병렬 스레드 수: {self.max_workers}")
        logger.info("=" * 60)

        # 1단계: URL 필터링
        logger.info("\n[1단계] URL 필터링")
        filtered_count = self.filter_urls()

        # 2단계: 크롤링
        logger.info("\n[2단계] 뉴스 크롤링")
        success_count, failed_count = self.crawl_news()

        # 실행 시간 계산
        elapsed_time = time.time() - start_time

        # 결과 요약
        logger.info("\n" + "=" * 60)
        logger.info(f"완료 - 필터링: {filtered_count:,}, 성공: {success_count:,}, 실패: {failed_count:,}")
        logger.info(f"총 실행 시간: {elapsed_time:.2f}초")
        if success_count > 0:
            logger.info(f"평균 처리 시간: {elapsed_time / success_count:.2f}초/건")
        logger.info("=" * 60)


if __name__ == "__main__":
    crawler = NewsCrawler(
        filter_file_path='filter_domain_list_v1.00.txt',
        crawler_version='1.0.0',
        max_workers=6  # 동시 실행할 스레드 수 (서버 부하 고려하여 조정)
    )

    # 모든 pending 데이터 처리
    crawler.run()
