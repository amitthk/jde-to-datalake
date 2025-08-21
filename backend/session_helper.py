"""
Session management for batch review data
"""
import uuid
import json
from datetime import datetime, timedelta
from jde_helper import get_db_connection

def create_batch_review_session(batch_data_list):
    """
    Create a session to store batch review data
    
    Args:
        batch_data_list: List of batch data dictionaries
        
    Returns:
        dict: {"success": bool, "session_id": str, "error": str}
    """
    try:
        print(f"Creating session with data: {batch_data_list}")
        
        # Validate input
        if not batch_data_list or not isinstance(batch_data_list, list):
            return {
                "success": False,
                "error": "batch_data_list must be a non-empty list"
            }
        
        session_id = str(uuid.uuid4())
        print(f"Generated session ID: {session_id}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Try to create table if it doesn't exist
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS batch_review_sessions (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(100) UNIQUE NOT NULL,
                    batch_data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP + INTERVAL '1 hour')
                );
            """)
            print("Table creation/check completed")
        except Exception as table_error:
            print(f"Table creation error: {table_error}")
        
        # Try to create cleanup function if it doesn't exist
        try:
            cur.execute("""
                CREATE OR REPLACE FUNCTION cleanup_expired_sessions()
                RETURNS void AS $$
                BEGIN
                    DELETE FROM batch_review_sessions WHERE expires_at < CURRENT_TIMESTAMP;
                END;
                $$ LANGUAGE plpgsql;
            """)
            print("Cleanup function creation completed")
        except Exception as func_error:
            print(f"Function creation error: {func_error}")
        
        # Clean up expired sessions first (but don't fail if function doesn't exist)
        try:
            cur.execute("SELECT cleanup_expired_sessions();")
            print("Cleanup executed")
        except Exception as cleanup_error:
            print(f"Cleanup warning (non-fatal): {cleanup_error}")
        
        # Store the batch data
        batch_json = json.dumps(batch_data_list)
        print(f"Storing batch data JSON: {batch_json[:200]}...")
        
        cur.execute("""
            INSERT INTO batch_review_sessions (session_id, batch_data)
            VALUES (%s, %s);
        """, (session_id, batch_json))
        
        conn.commit()
        conn.close()
        print(f"Session {session_id} created successfully")
        
        return {
            "success": True,
            "session_id": session_id
        }
    except Exception as e:
        print(f"Session creation error: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return {
            "success": False,
            "error": str(e)
        }

def get_batch_review_session(session_id):
    """
    Retrieve batch review data from session
    
    Args:
        session_id: Session ID to retrieve
        
    Returns:
        dict: {"success": bool, "data": list, "error": str}
    """
    try:
        print(f"Retrieving session: {session_id}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT batch_data FROM batch_review_sessions 
            WHERE session_id = %s AND expires_at > CURRENT_TIMESTAMP;
        """, (session_id,))
        
        result = cur.fetchone()
        conn.close()
        
        if result:
            batch_data_raw = result[0]
            print(f"Raw batch data type: {type(batch_data_raw)}")
            print(f"Raw batch data: {batch_data_raw}")
            
            # Handle different data types
            if isinstance(batch_data_raw, str):
                # If it's a string, parse as JSON
                batch_data = json.loads(batch_data_raw)
            elif isinstance(batch_data_raw, list):
                # If it's already a list, use directly
                batch_data = batch_data_raw
            else:
                # If it's something else (dict, etc.), try to convert
                batch_data = batch_data_raw
            
            return {
                "success": True,
                "data": batch_data
            }
        else:
            return {
                "success": False,
                "error": "Session not found or expired"
            }
    except Exception as e:
        print(f"Session retrieval error: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return {
            "success": False,
            "error": str(e)
        }

def delete_batch_review_session(session_id):
    """
    Delete a batch review session
    
    Args:
        session_id: Session ID to delete
        
    Returns:
        dict: {"success": bool, "error": str}
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            DELETE FROM batch_review_sessions 
            WHERE session_id = %s;
        """, (session_id,))
        
        conn.commit()
        conn.close()
        
        return {"success": True}
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
