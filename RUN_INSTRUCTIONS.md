# راه‌اندازی و اجرای ربات آربیتراژ Solana

## پیش‌نیازها

1. **Rust** (نسخه 1.70 یا بالاتر):
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. **Solana CLI** (برای اسکریپت‌های dump):
   ```bash
   sh -c "$(curl -sSfL https://release.anza.xyz/stable/install)"
   ```

3. **jq** (برای پردازش JSON در اسکریپت‌ها):
   ```bash
   apt-get install jq  # Ubuntu/Debian
   # یا
   brew install jq  # macOS
   ```

## ساختار پروژه

```
/workspace/
├── Cargo.toml              # پیکربندی پروژه Rust
├── config.toml             # فایل پیکربندی ربات
├── tokens.txt              # لیست توکن‌ها برای آربیتراژ
├── src/
│   ├── main.rs             # نقطه ورود اصلی
│   ├── program_registry.rs # ثبت صرافی‌ها و فایل‌های .so
│   ├── arbitrage.rs        # منطق اسکن آربیتراژ
│   ├── litesvm_sim.rs      # شبیه‌سازی LiteSVM
│   ├── account_cache.rs    # کش حساب‌ها از Yellowstone
│   ├── alt_cache.rs        # کش Address Lookup Tables
│   ├── transaction.rs      # ساخت تراکنش‌ها
│   └── bin/
│       └── alt_loader.rs   # ابزار مستقل بارگذاری ALT
├── scripts/
│   ├── redump_so.sh        # اسکریپت dump صرافی‌ها
│   └── test_dex_setup.sh   # اسکریپت تست مستقل
```

## صرافی‌های پشتیبانی شده

صرافی‌های فعلی در `program_registry.rs`:

| صرافی | Program ID | فایل .so |
|-------|------------|----------|
| Meteora DAMM v2 | `cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG` | Meteora_DAMM_v2.so |
| Raydium AMM v4 | `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` | Raydium_AMM_v4.so |
| Raydium CLMM | `CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK` | Raydium_Concentrated_Liquidity.so |
| Manifest | `MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms` | Manifest.so |
| Whirlpools | `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc` | Whirlpools_Program.so |
| BonkSwap | `BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p` | BonkSwap.so |
| Fusion AMM | `fUSioN9YKKSa3CUC2YUc4tPkHJ5Y6XW1yz8y6F7qWz9` | Fusion_AMM.so |
| Meteora Pools | `9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP` | Meteora_Pools_Program.so |
| Meteora DLMM | `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo` | Meteora_DLMM_Program.so |
| 1Dex | `DEXYosS6oEGvk8uCDayvwEZz4qEyDJRf9nFgYCaqPMTm` | 1Dex_Program.so |
| Invariant Swap | `24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi` | Invariant_Swap.so |
| PancakeSwap | `Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB` | PancakeSwap.so |
| Meteora Vault | `HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt` | Meteora_Vault_Program.so |
| Mercurial Stable Swap | `MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky` | Mercurial_Stable_Swap.so |
| Raydium CPMM | `CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C` | Raydium_CPMM.so |
| Jupiter Aggregator v6 | `JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4` | Jupiter_Aggregator_v6.so |

### افزودن/حذف صرافی

فقط کافی است فایل `src/program_registry.rs` را ویرایش کنید:

1. اضافه کردن صرافی جدید به `PROGRAMS`:
   ```rust
   ("NEW_PROGRAM_ID", "New_Dex_Program.so"),
   ```

2. حذف صرافی: خط مربوطه را از `PROGRAMS` پاک کنید

3. صرافی‌های ممنوعه را به `FORBIDDEN_DEX_PROGRAM_IDS` اضافه کنید

## مراحل راه‌اندازی

### 1. دانلود فایل‌های .so صرافی‌ها

```bash
# استفاده از اسکریپت خودکار
./scripts/redump_so.sh https://api.mainnet-beta.solana.com /path/to/so/dir

# یا استفاده از اسکریپت تست مستقل
./scripts/test_dex_setup.sh https://api.mainnet-beta.solana.com /path/to/so/dir
```

### 2. پیکربندی

فایل `config.toml` را ویرایش کنید:

```toml
[metis]
url = "http://127.0.0.1:8080"

[trading]
min_amount_sol = 0.1
max_amount_sol = 0.2
step_sol = 0.01
tokens_file = "tokens.txt"

[jito]
urls = [
    "https://amsterdam.mainnet.block-engine.jito.wtf",
    # ... سایر ریجن‌ها
]
trading_keypair = "/path/to/wallet.json"

[rpc]
url = "https://your-rpc-endpoint.com"

[yellowstone_grpc]
endpoint = "https://your-grpc-endpoint.com"
x_token = "your-x-token"

[simulation]
enabled = true
so_dir = "/path/to/so/dir"
fail_closed = true
```

### 3. ساخت پروژه

```bash
cargo build --release
```

### 4. اجرای ربات اصلی

```bash
cargo run --release
```

## ابزارهای کمکی

### 1. بارگذاری ALTها (مستقل)

این ابزار ALTهای مورد استفاده Jupiter را بررسی می‌کند:

```bash
cargo run --bin alt_loader -- https://api.mainnet-beta.solana.com
```

### 2. تست تنظیمات DEX

اسکریپت مستقل برای تست و تأیید صرافی‌ها:

```bash
./scripts/test_dex_setup.sh https://api.mainnet-beta.solana.com /path/to/so/dir
```

## رفع خطای AddressLookupTableNotFound

این خطا زمانی رخ می‌دهد که ALTها قبل از شبیه‌سازی در sim_cache بارگذاری نشده‌اند.

**راه‌حل:**
- ALTها باید در `litesvm_sim.rs` و در تابع `simulate()` با استفاده از `set_account()` به LiteSVM اضافه شوند
- کد فعلی این کار را در خطوط 227-238 انجام می‌دهد
- مطمئن شوید `alt_cache.get_or_fetch()` قبل از شبیه‌سازی فراخوانی می‌شود

## نکات مهم

1. **شبیه‌سازی**: اگر `fail_closed = true` باشد، تراکنش‌هایی که شبیه‌سازی آن‌ها ناموفق است ارسال نمی‌شوند

2. **Yellowstone gRPC**: برای دریافت آپدیت‌های لحظه‌ای حساب‌ها ضروری است

3. **Metis**: سرور Metis باید در حال اجرا باشد و به آدرس مشخص شده در config.toml متصل شود

4. **Wallet**: کیف پول باید دارای WSOL ATA باشد. اگر نیست:
   ```bash
   spl-token wrap <amount>
   ```

## لاگ‌گیری

برای تغییر سطح لاگ:

```bash
RUST_LOG=debug cargo run --release
# یا
RUST_LOG=solana_arb_bot=debug,cargo run --release
```
