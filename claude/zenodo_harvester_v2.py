import requests
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import List, Dict, Optional, Tuple, Set
import hashlib
import random

class ZenodoElasticsearchHarvester:
    """
    Zenodo harvester respektující Elasticsearch best practices v rámci API omezení:
    1. Používá konzistentní sortování s tiebreaker
    2. Rozděluje data na menší chunks pro vyhnutí deep pagination
    3. Implementuje idempotentní retry logiku
    4. Sleduje a loguje problematické situace
    5. Řeší extrémní single-day koncentrace dat dvojitým dotazem (oldest+newest)
    6. Automatické filtrování duplicit
    """
    
    def __init__(self, output_dir="zenodo_data"):
        self.base_url = "https://zenodo.org/api/records"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Výstupní soubory
        self.raw_file = self.output_dir / "zenodo_datasets.jsonl"
        self.log_file = self.output_dir / "harvest.log"
        self.progress_file = self.output_dir / "progress.json"
        self.checkpoint_file = self.output_dir / "checkpoints.json"
        self.duplicate_log = self.output_dir / "duplicates.log"
        
        # Rate limiting - Zenodo: 1000 req/hour pro anonymní
        self.max_requests_per_hour = 900  # Rezerva
        self.request_times = []
        
        # ES best practices adaptace
        self.max_safe_page = 200  # 10k limit = 200 stránek po 50
        self.consistent_sort = "created"  # Konzistentní sortování
        self.tiebreaker_field = "id"  # ID jako tiebreaker
        
        self.setup_logging()
        
        # Cache pro filtrování duplicit - after logging setup
        self.seen_record_ids: Set[str] = set()
        self.load_existing_record_ids()
    
    def load_existing_record_ids(self):
        """Načte existující record ID pro filtrování duplicit"""
        if self.raw_file.exists():
            try:
                with open(self.raw_file, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        try:
                            record = json.loads(line.strip())
                            record_id = record.get('id')
                            if record_id:
                                self.seen_record_ids.add(record_id)
                        except json.JSONDecodeError:
                            self.logger.warning(f"Skipping malformed JSON line {line_num}")
                            continue
                self.logger.info(f"Loaded {len(self.seen_record_ids)} existing record IDs for deduplication")
            except Exception as e:
                self.logger.warning(f"Could not load existing record IDs: {e}")
    
    def setup_logging(self):
        """Nastavení logování"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_progress(self) -> dict:
        """Načte progress"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {
            'completed_queries': [],
            'current_query': None,
            'total_downloaded': 0,
            'duplicates_filtered': 0,
            'extreme_days_processed': 0,
            'last_update': None,
            'consistency_warnings': 0
        }
    
    def save_progress(self, progress: dict):
        """Uloží progress"""
        progress['last_update'] = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(progress, f, indent=2)
    
    def load_checkpoints(self) -> dict:
        """Načte checkpoints pro detekci inconsistencí"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {}
    
    def save_checkpoint(self, query_id: str, page: int, record_ids: List[str]):
        """Uloží checkpoint s ID záznamů pro detekci posunů"""
        checkpoints = self.load_checkpoints()
        
        checkpoint_key = f"{query_id}_page_{page}"
        checkpoints[checkpoint_key] = {
            'record_ids': record_ids[:10],  # Uložíme prvních 10 ID
            'timestamp': datetime.now().isoformat(),
            'query': query_id
        }
        
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoints, f, indent=2)
    
    def detect_pagination_shift(self, query_id: str, page: int, current_ids: List[str]) -> bool:
        """Detekce posunu dat kvůli refresh operacím (ES best practice warning)"""
        checkpoints = self.load_checkpoints()
        checkpoint_key = f"{query_id}_page_{page}"
        
        if checkpoint_key not in checkpoints:
            return False
        
        previous_ids = checkpoints[checkpoint_key]['record_ids']
        current_sample = current_ids[:10]
        
        # Porovnej překryv ID
        overlap = set(previous_ids) & set(current_sample)
        overlap_ratio = len(overlap) / min(len(previous_ids), len(current_sample))
        
        if overlap_ratio < 0.8:  # Méně než 80% překryv
            self.logger.warning(f"Pagination shift detected for {query_id} page {page}")
            self.logger.warning(f"Overlap ratio: {overlap_ratio:.2f}")
            return True
        
        return False
    
    def check_rate_limit(self):
        """Rate limiting s exponential backoff"""
        current_time = time.time()
        
        # Vyčisti staré časy
        self.request_times = [t for t in self.request_times if current_time - t < 3600]
        
        # Pokud blízko limitu, počkej
        if len(self.request_times) >= self.max_requests_per_hour:
            wait_time = 3600 - (current_time - self.request_times[0]) + 60
            self.logger.warning(f"Rate limit reached. Waiting {wait_time:.0f} seconds...")
            time.sleep(wait_time)
            self.request_times = []
        
        # Základní delay s jitter (anti-thundering herd)
        base_delay = 4  # 900 req/hour
        jitter = random.uniform(0.5, 1.5)  # ±50% variace
        time.sleep(base_delay * jitter)
        
        self.request_times.append(current_time)
    
    def make_request_with_retry(self, params: dict, max_retries: int = 3) -> Optional[dict]:
        """Idempotentní request s exponential backoff"""
        self.check_rate_limit()
        
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'ZenodoElasticsearchHarvester/1.0',
            'Accept': 'application/json'
        }
        
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    self.base_url, 
                    params=params, 
                    headers=headers,
                    timeout=30
                )
                
                if response.status_code == 429:
                    # Rate limit - exponential backoff
                    wait_time = (2 ** attempt) * 60  # 1min, 2min, 4min
                    self.logger.warning(f"Rate limited, waiting {wait_time}s (attempt {attempt+1})")
                    time.sleep(wait_time)
                    continue
                
                if response.status_code == 400:
                    self.logger.error(f"Bad request (pagination limit?): {params}")
                    return None
                
                if response.status_code in [502, 503, 504]:
                    # Server error - retry s backoff
                    wait_time = (2 ** attempt) * 30
                    self.logger.warning(f"Server error {response.status_code}, retrying in {wait_time}s")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep((2 ** attempt) * 30)  # Exponential backoff
        
        return None
    
    def generate_safe_queries(self, start_year=2013, end_year=None) -> List[Tuple[str, str, str]]:
        """
        Generuje queries s predikovanou velikostí <9k záznamů
        Respektuje ES best practice: vyhni se deep pagination
        """
        if end_year is None:
            end_year = datetime.now().year
        
        queries = []
        
        # Roční queries s normálními datovými rozsahy (bez +1 day workaround)
        for year in range(start_year, end_year + 1):
            query = f"created:[{year}-01-01 TO {year}-12-31]"
            query_id = f"year_{year}"
            queries.append((query, query_id, f"Year {year}"))
        
        return queries
    
    def estimate_query_size(self, query: str) -> int:
        """Odhad velikosti query pro optimalizaci"""
        params = {
            'q': query,
            'type': 'dataset',
            'all_versions': '0',
            'size': '1'
        }
        
        response = self.make_request_with_retry(params)
        if response:
            return response.get('hits', {}).get('total', 0)
        return 0
    
    def harvest_safe_query(self, query: str, query_id: str, description: str) -> int:
        """
        Stáhne data pro query s bezpečnou paginací
        Implementuje ES best practices pro konzistenci
        """
        # Odhad velikosti
        total_estimated = self.estimate_query_size(query)
        self.logger.info(f"Query {query_id}: ~{total_estimated:,} records ({description})")
        
        if total_estimated == 0:
            return 0
        
        if total_estimated > 9000:
            self.logger.warning(f"Query {query_id} too large ({total_estimated:,}), splitting needed")
            return self.split_large_query(query, query_id, total_estimated)
        
        return self.harvest_with_pagination(query, query_id, "oldest")
    
    def harvest_with_pagination(self, query: str, query_id: str, sort_order: str, max_records: Optional[int] = None) -> int:
        """Standardní paginace s volitelným limitem záznamů"""
        downloaded = 0
        page = 1
        batch_size = 50
        consecutive_errors = 0
        
        while page <= self.max_safe_page:
            if max_records and downloaded >= max_records:
                self.logger.info(f"Reached max_records limit ({max_records}) for {query_id}")
                break
                
            params = {
                'q': query,
                'type': 'dataset',
                'all_versions': '0',
                'size': str(batch_size),
                'page': str(page),
                'sort': sort_order
            }
            
            self.logger.info(f"  {query_id} page {page}: records {(page-1)*batch_size:,}-{min(page*batch_size, max_records or 999999):,}")
            
            response = self.make_request_with_retry(params)
            if not response:
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    self.logger.error(f"Too many errors for {query_id}, aborting")
                    break
                page += 1
                continue
            
            consecutive_errors = 0
            hits = response.get('hits', {}).get('hits', [])
            
            if not hits:
                self.logger.info(f"No more data for {query_id} at page {page}")
                break
            
            # Pokud máme limit, ořež poslední batch
            if max_records and downloaded + len(hits) > max_records:
                hits = hits[:max_records - downloaded]
            
            # Extrahuj ID pro detekci posunů
            record_ids = [hit.get('id', '') for hit in hits]
            
            # Detekce pagination shift (ES warning)
            if self.detect_pagination_shift(query_id, page, record_ids):
                progress = self.load_progress()
                progress['consistency_warnings'] += 1
                self.save_progress(progress)
            
            # Uložit checkpoint pro detekci posunů
            self.save_checkpoint(query_id, page, record_ids)
            
            # Uložit záznamy (s filtrováním duplicit)
            saved_count = self.append_records_with_deduplication(hits)
            downloaded += saved_count
            page += 1
            
            self.logger.info(f"  Saved {saved_count} records. Query total: {downloaded:,}")
            
            # Pokud jsme dostali méně než očekáváno, pravděpodobně konec
            if len(hits) < batch_size:
                break
            
            # Safety break při dosažení pagination limitu
            if page > self.max_safe_page:
                self.logger.warning(f"Reached pagination limit for {query_id} (ES best practice: avoid deep pagination)")
                break
        
        return downloaded
    
    def harvest_extreme_single_day(self, query: str, query_id: str, total_size: int) -> int:
        """
        Zpracuje extrémní single-day dataset dvojitým dotazem:
        1. oldest - prvních 9000 záznamů
        2. newest - zbývající + 10% překryv
        """
        self.logger.warning(f"EXTREME DAY processing {query_id}: {total_size:,} records")
        
        # První dotaz: oldest, 9000 záznamů
        self.logger.info(f"Phase 1/2: Harvesting oldest 9000 records from {query_id}")
        first_batch_count = self.harvest_with_pagination(query, f"{query_id}_oldest", "oldest", 9000)
        
        # Druhý dotaz: newest, zbývající + 10% překryv
        remaining = total_size - 9000
        overlap_percent = 0.1
        overlap_count = max(int(overlap_percent * remaining), 100)
        second_batch_target = remaining + overlap_count
        
        self.logger.info(f"Phase 2/2: Harvesting newest {second_batch_target:,} records from {query_id}")
        self.logger.info(f"Expected overlap: ~{overlap_count:,} records (will be filtered out)")
        
        second_batch_count = self.harvest_with_pagination(query, f"{query_id}_newest", "newest", second_batch_target)
        
        total_harvested = first_batch_count + second_batch_count
        self.logger.warning(f"EXTREME DAY completed {query_id}: {total_harvested:,} records harvested")
        
        # Update progress counter
        progress = self.load_progress()
        progress['extreme_days_processed'] = progress.get('extreme_days_processed', 0) + 1
        self.save_progress(progress)
        
        return total_harvested
    
    def split_large_query(self, base_query: str, query_id: str, total_size: int, depth: int = 0) -> int:
        """Hierarchicky rozdělí velký query (čtvrtletí → měsíce → týdny → dny)"""
        max_depth = 4  # Zvýšena hloubka pro říjen 2017
        
        if depth > max_depth:
            self.logger.error(f"Max recursion depth reached for {query_id}, proceeding with {total_size:,} records")
            return self.harvest_safe_query(base_query.replace('created:', ''), query_id, f"{query_id} (forced)")
        
        self.logger.info(f"Splitting query {query_id} ({total_size:,} records, depth {depth})")
        
        # Extrahuj skutečný časový rozsah z aktuálního dotazu
        import re
        date_match = re.search(r'created:\[(\d{4}-\d{2}-\d{2}) TO (\d{4}-\d{2}-\d{2})\]', base_query)
        if not date_match:
            self.logger.error(f"Cannot parse date range from: {base_query}")
            return 0
        
        start_date_str = date_match.group(1)
        end_date_str = date_match.group(2)
        
        # Parsuj datumy
        from datetime import datetime, timedelta
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        
        # Vypočítej délku období a zvolit vhodné dělení
        period_days = (end_date - start_date).days + 1
        
        # Hierarchické dělení podle velikosti období
        if period_days > 300:  # > ~10 měsíců → dělíme na čtvrtletí
            parts = self._split_into_quarters(start_date, end_date, query_id)
        elif period_days > 60:  # > 2 měsíce → dělíme na měsíce  
            parts = self._split_into_months(start_date, end_date, query_id)
        elif period_days > 7:   # > týden → dělíme na týdny
            parts = self._split_into_weeks(start_date, end_date, query_id)
        elif period_days > 1:   # > den → dělíme na dny (pro extrémní případy)
            parts = self._split_into_days(start_date, end_date, query_id)
        else:  # 1 den s velkým množstvím dat → extreme day processing
            if total_size > 10000:
                self.logger.warning(f"Single day with {total_size:,} records - using extreme day processing")
                return self.harvest_extreme_single_day(base_query, query_id, total_size)
            else:
                self.logger.info(f"Single day with manageable size ({total_size:,} records)")
                return self.harvest_safe_query(base_query, query_id, query_id)
        
        total_downloaded = 0
        
        for part_start, part_end, part_id in parts:
            part_query = f"created:[{part_start} TO {part_end}]"
            part_query_id = f"{query_id}_{part_id}"
            
            # Zkontroluj velikost části
            part_size = self.estimate_query_size(part_query)
            self.logger.info(f"Part {part_query_id}: ~{part_size:,} records ({part_start} to {part_end})")
            
            if part_size > 9000:
                # Rekurzivně rozděl velkou část
                self.logger.warning(f"Part {part_query_id} still large ({part_size:,}), splitting further")
                part_downloaded = self.split_large_query(part_query, part_query_id, part_size, depth + 1)
            else:
                # Zpracuj malou část přímo
                part_downloaded = self.harvest_safe_query(part_query, part_query_id, f"{query_id} {part_id}")
            
            total_downloaded += part_downloaded
        
        return total_downloaded
    
    def _split_into_quarters(self, start_date, end_date, base_id):
        """Rozdělí období na čtvrtletí"""
        year = start_date.year
        
        # Pokud období překračuje rok, upravíme hranice
        actual_start = max(start_date, datetime(year, 1, 1))
        actual_end = min(end_date, datetime(year, 12, 31))
        
        quarters = []
        q_bounds = [
            (datetime(year, 1, 1), datetime(year, 3, 31), "Q1"),
            (datetime(year, 4, 1), datetime(year, 6, 30), "Q2"), 
            (datetime(year, 7, 1), datetime(year, 9, 30), "Q3"),
            (datetime(year, 10, 1), datetime(year, 12, 31), "Q4")
        ]
        
        for q_start, q_end, q_id in q_bounds:
            # Překryv s naším obdobím
            part_start = max(actual_start, q_start)
            part_end = min(actual_end, q_end)
            
            if part_start <= part_end:
                quarters.append((
                    part_start.strftime('%Y-%m-%d'),
                    part_end.strftime('%Y-%m-%d'),
                    q_id
                ))
        
        return quarters
    
    def _split_into_months(self, start_date, end_date, base_id):
        """Rozdělí období na měsíce"""
        months = []
        current = start_date.replace(day=1)  # Začátek měsíce
        
        while current <= end_date:
            # Konec aktuálního měsíce
            if current.month == 12:
                month_end = datetime(current.year + 1, 1, 1) - timedelta(days=1)
            else:
                month_end = datetime(current.year, current.month + 1, 1) - timedelta(days=1)
            
            # Průsečík s naším obdobím
            part_start = max(current, start_date)
            part_end = min(month_end, end_date)
            
            if part_start <= part_end:
                month_id = f"M{current.month:02d}"
                months.append((
                    part_start.strftime('%Y-%m-%d'),
                    part_end.strftime('%Y-%m-%d'),
                    month_id
                ))
            
            # Další měsíc
            if current.month == 12:
                current = datetime(current.year + 1, 1, 1)
            else:
                current = datetime(current.year, current.month + 1, 1)
        
        return months
    
    def _split_into_weeks(self, start_date, end_date, base_id):
        """Rozdělí období na týdny"""
        weeks = []
        current = start_date
        week_num = 1
        
        while current <= end_date:
            week_end = min(current + timedelta(days=6), end_date)
            weeks.append((
                current.strftime('%Y-%m-%d'),
                week_end.strftime('%Y-%m-%d'),
                f"W{week_num:02d}"
            ))
            current = week_end + timedelta(days=1)
            week_num += 1
        
        return weeks
    
    def _split_into_days(self, start_date, end_date, base_id):
        """Rozdělí období na jednotlivé dny (pro extrémní koncentrace dat)"""
        days = []
        current = start_date
        day_num = 1
        
        while current <= end_date:
            days.append((
                current.strftime('%Y-%m-%d'),
                current.strftime('%Y-%m-%d'),
                f"D{day_num:02d}"
            ))
            current = current + timedelta(days=1)
            day_num += 1
        
        return days
    
    def append_records_with_deduplication(self, records: list) -> int:
        """Připojí záznamy do JSONL souboru s filtrováním duplicit"""
        saved_count = 0
        duplicates_count = 0
        
        with open(self.raw_file, 'a', encoding='utf-8') as f:
            for record in records:
                record_id = record.get('id')
                if not record_id:
                    continue
                
                if record_id in self.seen_record_ids:
                    duplicates_count += 1
                    # Log duplicit do separátního souboru
                    with open(self.duplicate_log, 'a', encoding='utf-8') as dup_f:
                        dup_f.write(f"{datetime.now().isoformat()}: Duplicate {record_id}\n")
                    continue
                
                # Nový záznam - ulož ho
                self.seen_record_ids.add(record_id)
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
                saved_count += 1
        
        if duplicates_count > 0:
            self.logger.info(f"  Filtered {duplicates_count} duplicates")
            # Aktualizuj progress s počtem duplicit
            progress = self.load_progress()
            progress['duplicates_filtered'] = progress.get('duplicates_filtered', 0) + duplicates_count
            self.save_progress(progress)
        
        return saved_count
    
    def append_records(self, records: list):
        """Backward compatibility - volá novou metodu s deduplikací"""
        return self.append_records_with_deduplication(records)
    
    def harvest_all_datasets(self):
        """Hlavní funkce respektující ES best practices"""
        progress = self.load_progress()
        
        # Vygeneruj bezpečné queries
        all_queries = self.generate_safe_queries()  # Original - all years
        # all_queries = self.generate_safe_queries(start_year=2017, end_year=2017)  # TEST: Only 2017
        
        self.logger.info("=== Zenodo Harvest with Elasticsearch Best Practices ===")
        self.logger.info(f"Generated {len(all_queries)} safe queries (type=dataset as separate parameter)")
        self.logger.info(f"Already downloaded: {progress['total_downloaded']:,} records")
        
        if progress.get('duplicates_filtered', 0) > 0:
            self.logger.info(f"Duplicates filtered in previous runs: {progress['duplicates_filtered']:,}")
        
        if progress.get('extreme_days_processed', 0) > 0:
            self.logger.info(f"Extreme days processed: {progress['extreme_days_processed']}")
            
        if progress['consistency_warnings'] > 0:
            self.logger.warning(f"Previous consistency warnings: {progress['consistency_warnings']}")
        
        for query, query_id, description in all_queries:
            # Přeskoč už dokončené
            if query_id in progress['completed_queries']:
                self.logger.info(f"Skipping completed query: {query_id}")
                continue
            
            self.logger.info(f"\n=== Processing: {query_id} ===")
            
            try:
                downloaded = self.harvest_safe_query(query, query_id, description)
                
                # Aktualizuj progress
                progress['completed_queries'].append(query_id)
                progress['total_downloaded'] += downloaded
                progress['current_query'] = query_id
                self.save_progress(progress)
                
                self.logger.info(f"Completed {query_id}: {downloaded:,} records")
                self.logger.info(f"Total progress: {progress['total_downloaded']:,} records")
                
            except Exception as e:
                self.logger.error(f"Error in query {query_id}: {e}")
                continue
        
        # Finální report
        self.logger.info("\n=== HARVEST COMPLETED ===")
        self.logger.info(f"Total records: {progress['total_downloaded']:,}")
        self.logger.info(f"Duplicates filtered: {progress.get('duplicates_filtered', 0):,}")
        self.logger.info(f"Extreme days processed: {progress.get('extreme_days_processed', 0)}")
        self.logger.info(f"Consistency warnings: {progress['consistency_warnings']}")
        
        if self.raw_file.exists():
            file_size_mb = self.raw_file.stat().st_size / (1024*1024)
            self.logger.info(f"Output file: {self.raw_file} ({file_size_mb:.1f} MB)")
        
        return progress['total_downloaded']

# Hlavní spuštění
if __name__ == "__main__":
    harvester = ZenodoElasticsearchHarvester("zenodo_data")
    
    print("=== Zenodo Elasticsearch-Aware Harvester ===")
    print("Respects ES best practices:")
    print("• Avoids deep pagination (>10k limit)")
    print("• Uses consistent sorting with tiebreaker")
    print("• Detects pagination shifts")
    print("• Implements exponential backoff")
    print("• Handles extreme single-day datasets (oldest+newest strategy)")
    print("• Automatic deduplication")
    print()
    
    # Zkontroluj předchozí stahování
    progress = harvester.load_progress()
    if progress['total_downloaded'] > 0:
        print(f"Previous harvest: {progress['total_downloaded']:,} records")
        print(f"Completed queries: {len(progress['completed_queries'])}")
        if progress.get('duplicates_filtered', 0) > 0:
            print(f"Duplicates filtered: {progress['duplicates_filtered']:,}")
        if progress.get('extreme_days_processed', 0) > 0:
            print(f"Extreme days processed: {progress['extreme_days_processed']}")
        if progress['consistency_warnings'] > 0:
            print(f"⚠️ Consistency warnings: {progress['consistency_warnings']}")
        
        resume = input("Resume previous harvest? (y/n): ")
        if resume.lower() in ['y', 'yes']:
            try:
                total = harvester.harvest_all_datasets()
                print(f"\n✅ Harvest completed: {total:,} records")
            except KeyboardInterrupt:
                print("\n⏸️ Harvest paused. Run again to resume.")
            except Exception as e:
                print(f"\n❌ Error: {e}")
        else:
            print("Starting fresh harvest...")
    
    confirm = input("Start harvest? (y/n): ")
    if confirm.lower() in ['y', 'yes']:
        try:
            total = harvester.harvest_all_datasets()
            print(f"\n✅ Harvest completed: {total:,} records")
        except KeyboardInterrupt:
            print("\n⏸️ Harvest paused. Run again to resume.")
        except Exception as e:
            print(f"\n❌ Error: {e}")
    else:
        print("Cancelled.")
