import sys
from sqlalchemy import create_engine, text

try:
    from settings import DATABASE_URL
except ImportError:
    print("❌ ERROR: Could not load DATABASE_URL from settings.py")
    sys.exit(1)

if not DATABASE_URL:
    print("❌ ERROR: DATABASE_URL is empty in your .env or settings")
    sys.exit(1)

print(f"🔄 Attempting to connect to: {DATABASE_URL.split('@')[-1]} (password masked for security)")

try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        for row in result:
            pass
    print("✅ SUCCESS: Database connected perfectly!")
except Exception as e:
    print("❌ FAILED: Database connection failed!")
    print(f"Error details: {e}")
    sys.exit(1)
