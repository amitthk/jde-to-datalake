# debug_main.py
from main import get_joined_df3
import asyncio

if __name__ == "__main__":
    # Call the endpoint function directly
    result = asyncio.run(get_joined_df3())
    print(result)