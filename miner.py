#!/usr/bin/env python3
import requests
import hashlib
import time
import argparse
import random
import json
from datetime import datetime
import urllib3
import multiprocessing
import sys

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def mine_worker(worker_id, data, difficulty, start_nonce, end_nonce, result_queue, stop_event, hashes_counter):
    target = '0' * difficulty
    local_hashes = 0
    
    span = end_nonce - start_nonce + 1
    if span <= 0:
        return

    rng = random.Random()
    offset = rng.randint(0, span - 1)

    for i in range(span):
        nonce = start_nonce + ((offset + i) % span)

        if local_hashes >= 10000:
            with hashes_counter.get_lock():
                hashes_counter.value += local_hashes
            local_hashes = 0

            if stop_event.is_set():
                return

        test_data = f"{data}{nonce}"
        result_hash = hashlib.sha256(test_data.encode()).hexdigest()
        local_hashes += 1

        if result_hash.startswith(target):
            with hashes_counter.get_lock():
                hashes_counter.value += local_hashes
            result_queue.put((nonce, result_hash))
            stop_event.set()
            return

    with hashes_counter.get_lock():
        hashes_counter.value += local_hashes


class Miner:
    def __init__(self, api_url, api_token, cores, target_account=None):
        self.api_url = api_url.rstrip('/')
        self.headers = {
            'Authorization': f'Bearer {api_token}',
            'User-Agent': 'Scummybank-Miner/3.0',
            'Content-Type': 'application/json'
        }
        self.cores = cores
        self.target_account = target_account
        self.stats = {
            'tasks_completed': 0,
            'total_earned': 0.0,
            'hashes_computed': 0
        }

    def get_tasks_batch(self, difficulty, batch_size):
        params = {
            'difficulty': difficulty,
            'count': batch_size
        }
        try:
            response = requests.get(
                f'{self.api_url}/api/mining/task',
                headers=self.headers,
                params=params,
                verify=False,
                timeout=15
            )
            if response.status_code == 200:
                return response.json().get('tasks', [])
            elif response.status_code == 401:
                print("\n[FATAL] Unauthorized. Check your --token parameter.")
                sys.exit(1)
        except Exception as e:
            print(f"\n[ERROR] Failed to fetch tasks: {e}")
        return []

    def mine_single_task(self, task):
        difficulty = task['difficulty']
        task_id = task['id']
        raw_data = task['data']
        data = f"task_{task_id}_diff_{difficulty}_{raw_data}_"

        total_range = task['nonce_end'] - task['nonce_start'] + 1
        chunk_size = total_range // self.cores

        result_queue = multiprocessing.Queue()
        stop_event = multiprocessing.Event()
        hashes_counter = multiprocessing.Value('Q', 0)

        processes = []

        for i in range(self.cores):
            start_nonce = task['nonce_start'] + i * chunk_size
            end_nonce = task['nonce_end'] if i == self.cores - 1 else start_nonce + chunk_size - 1

            p = multiprocessing.Process(target=mine_worker, args=(
                i, data, difficulty, start_nonce, end_nonce,
                result_queue, stop_event, hashes_counter
            ))
            processes.append(p)
            p.start()

        start_time = time.time()
        last_update = start_time
        solution = None

        try:
            while any(p.is_alive() for p in processes):
                if not result_queue.empty():
                    solution = result_queue.get()
                    break

                current_time = time.time()
                if current_time - last_update > 0.5:
                    elapsed = current_time - start_time
                    current_hashes = hashes_counter.value
                    hashrate = current_hashes / elapsed if elapsed > 0 else 0

                    print(f"\r[BRRR] Solving Task #{task_id} | Diff: {difficulty} | Hashes: {current_hashes:,} | Rate: {hashrate:,.0f} H/s ", end='', flush=True)
                    last_update = current_time

                time.sleep(0.02)

            if solution is None and not result_queue.empty():
                solution = result_queue.get()

        except KeyboardInterrupt:
            stop_event.set()
            for p in processes:
                p.terminate()
                p.join()
            raise

        stop_event.set()
        for p in processes:
            p.join()

        self.stats['hashes_computed'] += hashes_counter.value

        if solution:
            nonce, result_hash = solution
            return nonce, result_hash

        return None, None

    def submit_solutions_batch(self, solutions):
        payload = {
            'solutions': solutions
        }
        if self.target_account:
            payload['account_number'] = self.target_account

        try:
            response = requests.post(
                f'{self.api_url}/api/mining/submit_batch',
                headers=self.headers,
                json=payload,
                verify=False,
                timeout=30
            )

            data = response.json()
            if response.status_code == 200:
                return True, data
            else:
                return False, data.get('error', f'HTTP {response.status_code}')
        except Exception as e:
            return False, str(e)

    def run(self, difficulty=2, batch_size=100):
        print(f"=== Scummy Bank High-Performance Batch Miner ===")
        print(f"API Host      : {self.api_url}")
        print(f"Difficulty    : Level {difficulty}")
        print(f"Batch Size    : {batch_size} tasks")
        print(f"Cores         : {self.cores} CPU Cores")
        if self.target_account:
            print(f"Target IBAN   : {self.target_account}")
        else:
            print(f"Target IBAN   : Default (Primary Account)")
        print("-" * 55)

        while True:
            try:
                print(f"[*] Requesting mountain of {batch_size} tasks from ledger...")
                tasks = self.get_tasks_batch(difficulty, batch_size)
                if not tasks:
                    print("[-] No tasks received. Sleeping for 5s...")
                    time.sleep(5)
                    continue

                print(f"[+] Loaded {len(tasks)} tasks. Starting calculation loop...")
                solutions = []

                for idx, task in enumerate(tasks):
                    now = datetime.now().strftime('%H:%M:%S')
                    print(f"[{now}] Processing task {idx + 1}/{len(tasks)} (ID: #{task['id']})")
                    
                    nonce, result_hash = self.mine_single_task(task)
                    
                    if nonce is not None:
                        solutions.append({
                            'task_id': task['id'],
                            'nonce': nonce,
                            'hash': result_hash
                        })
                        print(f"\n    -> Solved! Nonce: {nonce:,}")
                    else:
                        print(f"\n    -> Failed to solve (nonce exhausted)")

                if not solutions:
                    print("[-] Done with batch, but zero solutions found.")
                    continue

                print(f"[*] Submitting batch of {len(solutions)} solved tasks to ledger...")
                success, response = self.submit_solutions_batch(solutions)

                if success:
                    accepted = response.get('accepted_count', 0)
                    consolidated = response.get('consolidated_blocks', 0)
                    new_balance = response.get('new_balance', 0.0)
                    
                    print(f"[SUCCESS] Server accepted {accepted}/{len(solutions)} solutions.")
                    if consolidated > 0:
                        print(f"[BLOCKCHAIN] {consolidated} blocks were consolidated on disk! New balance: {new_balance:.2f} Funtiks")
                    else:
                        print(f"[ACCUMULATION] Rewards reserved! (Next blockchain consolidation at 1000 blocks).")
                    
                    self.stats['tasks_completed'] += accepted
                else:
                    print(f"[REJECTED] Batch submission failed: {response}")

                print("=" * 55)

            except KeyboardInterrupt:
                print(f"\n\n[STOP] Mining loop interrupted by user.")
                break
            except Exception as e:
                print(f"\n[ERROR] Connection error: {e}. Re-trying in 5 seconds...")
                time.sleep(5)

        print("\n" + "=" * 22 + " Session Stats " + "=" * 22)
        print(f"Total Tasks Solved & Submitted : {self.stats['tasks_completed']}")
        print(f"Total Hashes Computed          : {self.stats['hashes_computed']:,}")
        print("=" * 59)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scummy bank High-Performance Batch Miner')
    parser.add_argument('--url', default='https://scam.dn42', help='API Base URL')
    parser.add_argument('--token', required=True, help='User APIToken')
    parser.add_argument('--difficulty', type=int, default=2, choices=list(range(6, 11)),
                        help='Difficulty level (6-10)')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Number of tasks to fetch and solve in one loop iteration')
    parser.add_argument('--cores', type=int, default=multiprocessing.cpu_count(),
                        help='Number of CPU cores (default: max)')
    parser.add_argument('--account', default=None,
                        help='Target IBAN (DN420042... or 16 digits) to receive payouts')

    args = parser.parse_args()

    max_cores = multiprocessing.cpu_count()
    if args.cores > max_cores:
        args.cores = max_cores
    elif args.cores < 1:
        args.cores = 1

    target_iban = None
    if args.account:
        clean = args.account.replace(" ", "").replace("-", "").upper()
        if len(clean) == 16 and clean.isdigit():
            clean = f"DN420042{clean}"

        if not clean.startswith("DN420042") or len(clean) != 24:
            print(f"[FATAL] Invalid account format. Expected DN420042...")
            sys.exit(1)
        target_iban = clean

    miner = Miner(args.url, args.token, args.cores, target_account=target_iban)
    miner.run(difficulty=args.difficulty, batch_size=args.batch_size)