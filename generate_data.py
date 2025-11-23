import csv
import random
import time
import sys # Import for sys.stderr for logging

# --- CONFIGURATION ---
NUM_ROWS = 500_000 # Start with 500k, increase to 1M if you want to suffer
FILENAME = "dirty_catalogue.csv"

# --- BASE DATA FOR GENERATION ---
BRANDS = ["Apple", "Samsung", "Sony", "Dell", "HP", "Logitech", "Asus", "Lenovo"]
PRODUCTS = ["iPhone 13", "Galaxy S21", "PlayStation 5", "XPS 13", "Monitor 24", "Mouse MX", "ThinkPad", "MacBook Pro"]
SUFFIXES = ["Pro", "Max", "Mini", "Ultra", "Edition", "Lite"]
SUPPLIERS = ["Amazon", "Cdiscount", "Fnac", "Darty", "Boulanger", "RueDuCommerce"]

# --- HELPER FUNCTIONS ---

def log_progress(message):
    """Prints a progress message to the console (stderr for better logging separation)."""
    # Using sys.stderr to not interfere with potential output redirection
    print(f"[INFO] {message}", file=sys.stderr)

def corrupt_price(base_price: float) -> str:
    """Makes the price dirty (string, symbols, errors)"""
    r = random.random()
    if r < 0.1: return f"${base_price:.2f}" # Dollar symbol
    if r < 0.2: return f"{base_price:.2f} eur" # Suffix
    if r < 0.3: return f"{base_price:.2f}".replace('.', ',') # French comma separator
    if r < 0.35: return "" # Missing price
    if r < 0.4: return str(round(base_price * 100)) # Decimal error (too expensive)
    if r < 0.42: return "0" # Free (error)
    return f"{base_price:.2f}"

def corrupt_name(name: str) -> str:
    """Adds typos or changes casing"""
    r = random.random()
    if r < 0.3: return name.lower() # All lowercase
    if r < 0.5: return name.upper() # All uppercase
    if r < 0.6: return name.replace(" ", "  ") # Double space
    if r < 0.7: # Typo (single character removal)
        if len(name) > 1:
            idx = random.randint(0, len(name) - 1)
            return name[:idx] + name[idx + 1:] 
        return name
    return name

# --- MAIN GENERATION SCRIPT ---

if __name__ == "__main__":
    log_progress(f"Starting generation of {NUM_ROWS:,} dirty data rows...")
    start_time = time.time()

    try:
        with open(FILENAME, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            # Write header row
            writer.writerow(["ID_Source", "Product_Name", "Price_Raw", "Supplier_Name", "Date_Scraped"])

            for i in range(NUM_ROWS):
                # Create a base product
                brand = random.choice(BRANDS)
                prod = random.choice(PRODUCTS)
                suffix = random.choice(SUFFIXES) if random.random() > 0.5 else ""
                full_name = f"{brand} {prod} {suffix}".strip()
                
                # Consistent base price (with two decimals for better corruption)
                base_price = round(random.uniform(50.00, 2000.00), 2)
                
                # Corrupt the data
                dirty_name = corrupt_name(full_name)
                dirty_price = corrupt_price(base_price)
                supplier = random.choice(SUPPLIERS)
                
                # Random date in October 2023
                day = random.randint(1, 28)
                date = f"2023-10-{day:02d}" # Ensures two digits for day

                writer.writerow([i, dirty_name, dirty_price, supplier, date])

                if (i + 1) % 100_000 == 0:
                    log_progress(f"{i + 1:,} rows generated...")

        end_time = time.time()
        log_progress("-------------------------------------------")
        log_progress(f"✅ Success! File '{FILENAME}' generated with {NUM_ROWS:,} rows.")
        log_progress(f"Total time: {end_time - start_time:.2f} seconds.")
        log_progress("Your turn now: clean this mess in under 2 minutes!")
        
    except IOError as e:
        log_progress(f"[ERROR] Could not write to file {FILENAME}: {e}")
    except Exception as e:
        log_progress(f"[CRITICAL ERROR] An unexpected error occurred: {e}")
