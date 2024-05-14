from notion_client import Client
from dotenv import load_dotenv
import datetime
import os

load_dotenv()
notion = Client(auth= os.getenv("token_notion"))
database_id = os.getenv("db_credentials")

def check_events():
    
    today = datetime.datetime.now

    #Query buscando base dos eventos
    response = notion.database.query (
        {
            "database_id": database_id,
            "filter": {
                "property": "Date",
                "date": {
                    "past_week": {}
                }
            }
        }
    )

    for page in response.get("results", []):

        event_date = datetime.datetime.fromisoformat(page["properties"]["Data"]["date"]["start"])
        days_diff = (today - event_date).days

        if days_diff > 3:
            notion.pages.update (
                {
                    "page_id": page["id"],
                    "archived": True
                }
            )
            print(f"Evento '{page['properties']['Name']['title'][0]['text']['content']}' arquivado.")


    check_events()
