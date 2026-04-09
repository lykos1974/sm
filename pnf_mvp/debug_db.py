import sqlite3

conn = sqlite3.connect("strategy_validation.db")
cur = conn.cursor()

print("ALL STATUSES:")
for row in cur.execute("""
    SELECT status, COUNT(*)
    FROM strategy_setups
    GROUP BY status
    ORDER BY COUNT(*) DESC
"""):
    print(row)

print("\nRESOLUTION STATUSES:")
for row in cur.execute("""
    SELECT resolution_status, COUNT(*)
    FROM strategy_setups
    GROUP BY resolution_status
    ORDER BY COUNT(*) DESC
"""):
    print(row)

conn.close()