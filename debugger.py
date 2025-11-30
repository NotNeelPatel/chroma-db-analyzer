import chromadb
import os
from collections import Counter

# Change this
DB_PATH = "" 

def inspect_chroma_db():
    print(f"Connecting to ChromaDB at: {os.path.abspath(DB_PATH)}")
    
    try:
        # Initialize the Client
        client = chromadb.PersistentClient(path=DB_PATH)
        
        # 1. List Collections
        collections_list = client.list_collections()
        
        if not collections_list:
            print("\nNo collections found! Are you pointing to the right folder?")
            return

        print(f"\nFound {len(collections_list)} collection(s).")

        for col_obj in collections_list:
            col_name = col_obj.name
            collection = client.get_collection(name=col_name)
            count = collection.count()
            
            print(f"\n" + "="*50)
            print(f"COLLECTION: '{col_name}' (Contains {count} items)")
            print("="*50)

            if count == 0:
                print("   (Empty Collection)")
                continue

            # 2. Peek at Data (First 3 Items)
            print("\nPEEKING AT FIRST 3 ITEMS:")
            peek_data = collection.peek(limit=3)
            
            # Helper to safely handle None values if metadata is missing
            ids = peek_data.get('ids', [])
            docs = peek_data.get('documents', [])
            metas = peek_data.get('metadatas', [])

            for i in range(len(ids)):
                print(f"   [Item {i}]")
                print(f"   ├── ID:       {ids[i]}")
                # Print only first 100 chars of document to avoid spamming console
                doc_preview = docs[i][:100].replace('\n', ' ') + "..." if docs[i] else "EMPTY/NONE"
                print(f"   ├── Document: {doc_preview}")
                print(f"   └── Metadata: {metas[i]}")
            
            # 3. Health Check (Empty & Duplicates)
            print("\nRUNNING HEALTH CHECK...")
            
            # Fetch all IDs and Documents for analysis
            # Warning: For massive DBs (millions of items), this might be slow.
            all_data = collection.get(include=["documents"])
            all_ids = all_data['ids']
            all_docs = all_data['documents']

            # Check 1: Empty Documents
            empty_count = 0
            empty_ids = []
            for doc_id, doc_text in zip(all_ids, all_docs):
                if not doc_text or doc_text.strip() == "":
                    empty_count += 1
                    if len(empty_ids) < 3: empty_ids.append(doc_id)

            if empty_count > 0:
                print(f"WARNING: Found {empty_count} items with EMPTY text content.")
                print(f"Sample IDs: {empty_ids}")
            else:
                print("     Text Content: No empty documents found.")

            # Check 2: Duplicate Content
            # We count occurrences of every document string
            doc_counts = Counter(all_docs)
            duplicates = {k: v for k, v in doc_counts.items() if v > 1}

            if duplicates:
                print(f"WARNING: Found {len(duplicates)} documents that appear multiple times.")
                print("(This suggests you might be embedding the same text under different IDs)")
                
                # Show top 2 duplicates
                sorted_dupes = sorted(duplicates.items(), key=lambda x: x[1], reverse=True)[:2]
                for text, count in sorted_dupes:
                    preview = text[:50].replace('\n', ' ')
                    print(f"      - \"{preview}...\" appears {count} times")
            else:
                print("Duplication: No exact duplicate text content found.")

    except Exception as e:
        print("\nError:")
        print(e)
        print("\nTry checking if your DB_PATH is correct.")
        print("Note: PersistentClient expects the FOLDER path, not the .sqlite3 file path.")

if __name__ == "__main__":
    inspect_chroma_db()
