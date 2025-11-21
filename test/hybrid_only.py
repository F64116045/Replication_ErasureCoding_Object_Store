import requests
import argparse
import sys
from typing import Any

# --- Color constants for terminal output ---
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    BOLD = '\033[1m'
    RESET = '\033[0m'

# --- Configuration ---
BASE_URL = "http://127.0.0.1:8000"

# English Test Data
TEST_DATA = {
    "like_count": 1500,
    "view_count": 50000,
    "inventory_count": 100,
    "update_count": 25,
    "content": "This is a test article content with a significant amount of text data." * 10,
    "author_id": "user_12345",
    "description": "Product description text",
    "specifications": {"size": "L", "color": "blue", "material": "cotton"},
    "create_time": "2025-10-31T12:00:00Z"
}

# --- TestRunner Class ---
class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.total_tests = 0
    
    def print_header(self, title: str):
        print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'=' * 70}{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.MAGENTA}{title:^70}{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.MAGENTA}{'=' * 70}{Colors.RESET}\n")
    
    def print_section(self, title: str):
        print(f"\n{Colors.BOLD}{Colors.CYAN}► {title}{Colors.RESET}")
        print(f"{Colors.CYAN}{'-' * 70}{Colors.RESET}")
    
    def assert_test(self, condition: bool, test_name: str, details: str = ""):
        self.total_tests += 1
        if condition:
            self.passed += 1
            print(f"{Colors.GREEN}✓ PASS{Colors.RESET} | {test_name}")
            if details:
                print(f"  {Colors.CYAN}→ {details}{Colors.RESET}")
        else:
            self.failed += 1
            print(f"{Colors.RED}✗ FAIL{Colors.RESET} | {test_name}")
            if details:
                print(f"  {Colors.RED}→ {details}{Colors.RESET}")
    
    def print_summary(self):
        print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'=' * 70}{Colors.RESET}")
        print(f"{Colors.BOLD}Test Summary{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.MAGENTA}{'=' * 70}{Colors.RESET}")
        print(f"Total Tests: {self.total_tests}")
        print(f"{Colors.GREEN}Passed: {self.passed}{Colors.RESET}")
        print(f"{Colors.RED}Failed: {self.failed}{Colors.RESET}")
        
        if self.failed == 0:
            print(f"\n{Colors.GREEN}{Colors.BOLD}All Hybrid tests passed!{Colors.RESET}\n")
        else:
            print(f"\n{Colors.RED}{Colors.BOLD}⚠ Some tests failed!{Colors.RESET}\n")

# --- System Check Functions ---
def check_system_health():
    """Check if the storage system is running"""
    print(f"{Colors.YELLOW}Checking system health (Target: {BASE_URL})...{Colors.RESET}")
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            print(f"{Colors.GREEN}✓ System is running{Colors.RESET}")
            return True
        else:
            print(f"{Colors.RED}✗ System status abnormal ({response.status_code}){Colors.RESET}")
            return False
    except Exception as e:
        print(f"{Colors.RED}✗ Cannot connect to system: {e}{Colors.RESET}")
        return False

def cleanup_test_keys(runner: TestRunner):
    """Clean up test keys before starting"""
    runner.print_section("Pre-Test Cleanup")
    key = "test_hybrid"
    try:
        requests.delete(f"{BASE_URL}/delete/{key}", timeout=5)
    except Exception:
        pass 
    print(f"{Colors.CYAN}✓ Cleanup of '{key}' complete{Colors.RESET}")

# --- Core Test Logic ---
def test_field_hybrid_strategy(runner: TestRunner):
    """Test Strategy C: Field-level Hybrid"""
    runner.print_section("Strategy C: Field-level Hybrid")
    
    key = "test_hybrid"
    
    # --- 1. Initial Write Test ---
    try:
        response = requests.post(
            f"{BASE_URL}/write",
            params={"key": key, "strategy": "field_hybrid"},
            json=TEST_DATA
        )
        data = response.json()
        runner.assert_test(
            response.status_code == 200,
            "1. Initial Write (Hybrid)",
            f"Operation: {data.get('operation_type')}"
        )
    except Exception as e:
        runner.assert_test(False, "1. Initial Write (Hybrid)", f"Error: {e}")
        return # Stop if initial write fails

    # --- 2. Read Verification ---
    response_data = None
    try:
        response = requests.get(f"{BASE_URL}/read/{key}")
        
        if response.status_code != 200:
            runner.assert_test(
                False,
                "2. Read Data (Hybrid)",
                f"HTTP Status: {response.status_code} | Error: {response.text}"
            )
        else:
            response_data = response.json() 
            data_match = response_data == TEST_DATA
            runner.assert_test(
                data_match,
                "2. Read Data (Hybrid)",
                f"Exact Match: {data_match}"
            )
            if not data_match:
                print(f"  {Colors.RED}→ Expected: {str(TEST_DATA)[:100]}...{Colors.RESET}")
                print(f"  {Colors.RED}→ Received: {str(response_data)[:100]}...{Colors.RESET}")
    
    except Exception as e:
        runner.assert_test(False, "2. Read Data (Hybrid)", f"Error: {e}")
    
    # --- 3. Hot Update Test ---
    updated_data_hot = TEST_DATA.copy()
    updated_data_hot['like_count'] = 2000
    updated_data_hot['view_count'] = 60000
    
    try:
        response = requests.post(
            f"{BASE_URL}/write",
            params={"key": key, "strategy": "field_hybrid", "hot_only": True},
            json=updated_data_hot
        )
        data = response.json()
        runner.assert_test(
            response.status_code == 200 and data.get('is_pure_hot_update'),
            "3. Hot Update (Hot fields only)",
            f"Pure Hot Update: {data.get('is_pure_hot_update')}, Cold Chunks Written: {data.get('cold_chunks_written')}"
        )
    except Exception as e:
        runner.assert_test(False, "3. Hot Update", f"Error: {e}")
    
    # --- 4. Verify Hot Update ---
    try:
        response = requests.get(f"{BASE_URL}/read/{key}")
        
        if response.status_code != 200:
            runner.assert_test(
                False,
                "4. Verify Hot Update Result",
                f"HTTP Status: {response.status_code}"
            )
        else:
            data = response.json()
            hot_updated = data.get('like_count') == 2000 and data.get('view_count') == 60000
            cold_unchanged = data.get('content') == TEST_DATA['content']
            
            runner.assert_test(
                hot_updated and cold_unchanged,
                "4. Verify Hot Update Result",
                f"Hot Updated: {hot_updated}, Cold Unchanged: {cold_unchanged}"
            )
            if not hot_updated:
                 print(f"  {Colors.RED}→ Hot field check failed: like_count={data.get('like_count')} (Expected 2000){Colors.RESET}")
            if not cold_unchanged:
                 print(f"  {Colors.RED}→ Cold field check failed: content changed{Colors.RESET}")

    except Exception as e:
        runner.assert_test(False, "4. Verify Hot Update Result", f"Error: {e}")

    # --- 5. Mixed Update Test ---
    updated_data_mixed = TEST_DATA.copy()
    updated_data_mixed['like_count'] = 2500
    updated_data_mixed['description'] = "This is a new description"
    
    try:
        response = requests.post(
            f"{BASE_URL}/write",
            params={"key": key, "strategy": "field_hybrid"}, 
            json=updated_data_mixed
        )
        data = response.json()
        runner.assert_test(
            response.status_code == 200 and not data.get('is_pure_hot_update'),
            "5. Mixed Update (Hot + Cold fields)",
            f"Should trigger Cold Write (Pure Hot Update: {data.get('is_pure_hot_update')})"
        )
    except Exception as e:
        runner.assert_test(False, "5. Mixed Update", f"Error: {e}")

    # --- 6. Verify Mixed Update ---
    try:
        response = requests.get(f"{BASE_URL}/read/{key}")
        
        if response.status_code != 200:
             runner.assert_test(
                False,
                "6. Verify Mixed Update Result",
                f"HTTP Status: {response.status_code}"
            )
        else:
            data = response.json()
            hot_updated = data.get('like_count') == 2500
            cold_updated = data.get('description') == "This is a new description"
            
            runner.assert_test(
                hot_updated and cold_updated,
                "6. Verify Mixed Update Result",
                f"Hot Updated: {hot_updated}, Cold Updated: {cold_updated}"
            )
            if not hot_updated:
                 print(f"  {Colors.RED}→ Hot check failed: like_count={data.get('like_count')} (Expected 2500){Colors.RESET}")
            if not cold_updated:
                 print(f"  {Colors.RED}→ Cold check failed: description={data.get('description')}{Colors.RESET}")
                 
    except Exception as e:
        runner.assert_test(False, "6. Verify Mixed Update Result", f"Error: {e}")
        
    # --- 7. Delete Test ---
    try:
        response = requests.delete(f"{BASE_URL}/delete/{key}")
        data = response.json()
        runner.assert_test(
            response.status_code == 200,
            "7. Delete Data",
            f"Strategy: {data.get('strategy', 'N/A')}"
        )
    except Exception as e:
        runner.assert_test(False, "7. Delete Data", f"Error: {e}")

# --- Main Execution ---
def main():
    global BASE_URL
    
    parser = argparse.ArgumentParser(description="Hybrid Strategy Debug Script")
    parser.add_argument(
        "--base-url",
        type=str,
        default="http://127.0.0.1:8000",
        help="API Base URL"
    )
    args = parser.parse_args()
    BASE_URL = args.base_url 
    
    runner = TestRunner()
    runner.print_header(f"Hybrid Strategy Debugger\nTarget: {BASE_URL}")
    
    if not check_system_health():
        print(f"\n{Colors.RED}Error: System is not running. Please start the server first.{Colors.RESET}\n")
        sys.exit(1)
    
    cleanup_test_keys(runner)
    
    print(f"\n{Colors.GREEN}Starting Hybrid Strategy Tests...{Colors.RESET}")
    
    test_field_hybrid_strategy(runner)
    
    runner.print_summary()
    
    if runner.failed > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()