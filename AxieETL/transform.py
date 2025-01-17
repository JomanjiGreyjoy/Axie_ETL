import os
import json
import pandas as pd
import logging
from azure.storage.blob import BlobServiceClient
from datetime import datetime

def transform_data(raw_blob_name: str) -> list:
    """
    Reads the raw JSON from Blob, transforms into DataFrame(s), writes
    transformed JSON back to Blob. Returns the new transformed blob name.
    """
    # 1) Connect to Blob Storage
    blob_conn_str = os.getenv("BLOB_CONNECTION_STRING", "")
    blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)

    raw_container = "axie-data"
    transformed_container = "axie-transformed-data"

    container_client = blob_service_client.get_container_client(raw_container)
    blob_client = container_client.get_blob_client(raw_blob_name)
    raw_json = json.loads(blob_client.download_blob().readall())

    # 2) Check for valid data structure
    if not raw_json.get("data"):
        logging.warning("No 'data' field in JSON. Returning empty transforms.")
        return "NO_DATA"

    settled_auctions = raw_json["data"].get("settledAuctions")
    if not settled_auctions or not settled_auctions.get("axies"):
        logging.warning("No 'settledAuctions' or 'axies' object in JSON. Returning empty transforms.")
        return "NO_DATA"

    axies_data = settled_auctions["axies"]
    if not axies_data or not axies_data.get("results"):
        logging.warning("'axies' has no 'results'. Returning empty transforms.")
        return "NO_DATA"

    listed_axies = axies_data["results"]
    logging.info(f"Number of axies in raw data: {len(listed_axies)}")

    # 3) Initialize data structures for transformations
    all_axies_list = []

    # Parts
    eyes_parts_dict = {}
    mouth_parts_dict = {}
    ears_parts_dict = {}
    horn_parts_dict = {}
    back_parts_dict = {}
    tail_parts_dict = {}

    # Abilities
    mouth_ability_dict = {}
    horn_ability_dict = {}
    back_ability_dict = {}
    tail_ability_dict = {}

    # Facts
    all_offers_list = []
    all_orders_list = []
    all_transfers_list = []

    # 4) Parse each Axie record
    for axie in listed_axies:
        # ---------------------------------------------------------
        # 4A) Basic Axie info
        # ---------------------------------------------------------
        axie_dict = {
            "id": axie["id"],
            "birth_date": axie["birthDate"],
            "body_shape": axie["bodyShape"],
            "class": axie["class"],
            "color": axie["primaryColor"],
            "pureness": axie["pureness"],
            "purity": axie["purity"],
            "stage": axie["stage"],
            "hp": axie["stats"]["hp"],
            "morale": axie["stats"]["morale"],
            "skill": axie["stats"]["skill"],
            "speed": axie["stats"]["speed"]
        }

        # ---------------------------------------------------------
        # 4B) Parse Parts & Abilities
        # ---------------------------------------------------------
        parts = axie.get("parts", [])
        abilities = []
        for part in parts:
            # Collect abilities from each part (if any)
            if part.get("abilities"):
                for ability in part["abilities"]:
                    abilities.append(ability)

            part_id = part["id"]
            part_type = part["type"]
            part_name = part["name"]
            part_class = part["class"]

            # Assign part_id in axie_dict & track unique part info
            if part_type == "Eyes":
                axie_dict["eyes_id"] = part_id
                if part_id not in eyes_parts_dict:
                    eyes_parts_dict[part_id] = {
                        "eyes_name": part_name,
                        "eyes_class": part_class,
                        "eyes_type": part_type
                    }
            elif part_type == "Mouth":
                axie_dict["mouth_id"] = part_id
                if part_id not in mouth_parts_dict:
                    mouth_parts_dict[part_id] = {
                        "mouth_name": part_name,
                        "mouth_class": part_class,
                        "mouth_type": part_type
                    }
            elif part_type == "Ears":
                axie_dict["ears_id"] = part_id
                if part_id not in ears_parts_dict:
                    ears_parts_dict[part_id] = {
                        "ears_name": part_name,
                        "ears_class": part_class,
                        "ears_type": part_type
                    }
            elif part_type == "Horn":
                axie_dict["horn_id"] = part_id
                if part_id not in horn_parts_dict:
                    horn_parts_dict[part_id] = {
                        "horn_name": part_name,
                        "horn_class": part_class,
                        "horn_type": part_type
                    }
            elif part_type == "Back":
                axie_dict["back_id"] = part_id
                if part_id not in back_parts_dict:
                    back_parts_dict[part_id] = {
                        "back_name": part_name,
                        "back_class": part_class,
                        "back_type": part_type
                    }
            elif part_type == "Tail":
                axie_dict["tail_id"] = part_id
                if part_id not in tail_parts_dict:
                    tail_parts_dict[part_id] = {
                        "tail_name": part_name,
                        "tail_class": part_class,
                        "tail_type": part_type
                    }

        # Parse abilities (mouth/horn/back/tail, if present)
        for ability in abilities:
            ability_id = ability["id"]
            ability_name = ability["name"]
            ability_attack = ability["attack"]
            ability_defense = ability["defense"]
            ability_energy = ability["energy"]
            ability_description = ability["description"]

            if 'mouth' in ability_id:
                axie_dict["mouth_ability_id"] = ability_id
                if ability_id not in mouth_ability_dict:
                    mouth_ability_dict[ability_id] = {
                        "mouth_ability_name": ability_name,
                        "mouth_ability_attack": ability_attack,
                        "mouth_ability_defense": ability_defense,
                        "mouth_ability_energy": ability_energy,
                        "mouth_ability_description": ability_description
                    }
            elif 'horn' in ability_id:
                axie_dict["horn_ability_id"] = ability_id
                if ability_id not in horn_ability_dict:
                    horn_ability_dict[ability_id] = {
                        "horn_ability_name": ability_name,
                        "horn_ability_attack": ability_attack,
                        "horn_ability_defense": ability_defense,
                        "horn_ability_energy": ability_energy,
                        "horn_ability_description": ability_description
                    }
            elif 'back' in ability_id:
                axie_dict["back_ability_id"] = ability_id
                if ability_id not in back_ability_dict:
                    back_ability_dict[ability_id] = {
                        "back_ability_name": ability_name,
                        "back_ability_attack": ability_attack,
                        "back_ability_defense": ability_defense,
                        "back_ability_energy": ability_energy,
                        "back_ability_description": ability_description
                    }
            elif 'tail' in ability_id:
                axie_dict["tail_ability_id"] = ability_id
                if ability_id not in tail_ability_dict:
                    tail_ability_dict[ability_id] = {
                        "tail_ability_name": ability_name,
                        "tail_ability_attack": ability_attack,
                        "tail_ability_defense": ability_defense,
                        "tail_ability_energy": ability_energy,
                        "tail_ability_description": ability_description
                    }

        # Add this axie to the main list
        all_axies_list.append(axie_dict)

        # ---------------------------------------------------------
        # 4C) Offers
        # ---------------------------------------------------------
        offers = axie.get("offers", {}).get("data", [])
        for offer in offers:
            offer_dict = {
                "offer_id": offer["id"],
                "axie_id": axie_dict["id"],
                "added_at": offer["addedAt"],
                "ended_at": offer["endedAt"],
                "status": offer["status"],
                "payment_token": offer["paymentToken"],
                "base_price": offer["basePrice"],
                "current_price_usd": offer["currentPriceUsd"],
                "current_price": offer["currentPrice"],
                "ended_price": offer["endedPrice"]
            }
            all_offers_list.append(offer_dict)

        # ---------------------------------------------------------
        # 4D) Transfers
        # ---------------------------------------------------------
        transfers = axie.get("transferHistory", {}).get("results", [])
        for t in transfers:
            transfer_dict = {
                "transfer_id": t["txHash"],
                "axie_id": axie_dict["id"],
                "timestamp": t["timestamp"],
                "with_price_usd": t["withPriceUsd"],
                "with_price": t["withPrice"]
            }
            all_transfers_list.append(transfer_dict)

        # ---------------------------------------------------------
        # 4E) Order
        # ---------------------------------------------------------
        order = axie.get("order")
        if order:
            order_dict = {
                "order_id": order["id"],
                "axie_id": axie_dict["id"],
                "added_at": order["addedAt"],
                "ended_at": order["endedAt"],
                "status": order["status"],
                "payment_token": order["paymentToken"],
                "base_price": order["basePrice"],
                "current_price_usd": order["currentPriceUsd"],
                "current_price": order["currentPrice"],
                "ended_price": order["endedPrice"]
            }
            all_orders_list.append(order_dict)

    # 5) Convert python lists/dicts -> DataFrames
    all_axies_df = pd.DataFrame(all_axies_list)
    all_axies_df.drop_duplicates(subset=["id"], inplace=True)
    all_axies_df.dropna(subset=["id"], inplace=True)

    # Parts
    eyes_parts_df = pd.DataFrame.from_dict(eyes_parts_dict, orient='index')
    eyes_parts_df.index.name = "eyes_id"
    eyes_parts_df.reset_index(inplace=True)

    mouth_parts_df = pd.DataFrame.from_dict(mouth_parts_dict, orient='index')
    mouth_parts_df.index.name = "mouth_id"
    mouth_parts_df.reset_index(inplace=True)

    ears_parts_df = pd.DataFrame.from_dict(ears_parts_dict, orient='index')
    ears_parts_df.index.name = "ears_id"
    ears_parts_df.reset_index(inplace=True)

    horn_parts_df = pd.DataFrame.from_dict(horn_parts_dict, orient='index')
    horn_parts_df.index.name = "horn_id"
    horn_parts_df.reset_index(inplace=True)

    back_parts_df = pd.DataFrame.from_dict(back_parts_dict, orient='index')
    back_parts_df.index.name = "back_id"
    back_parts_df.reset_index(inplace=True)

    tail_parts_df = pd.DataFrame.from_dict(tail_parts_dict, orient='index')
    tail_parts_df.index.name = "tail_id"
    tail_parts_df.reset_index(inplace=True)

    # Abilities
    mouth_ability_df = pd.DataFrame.from_dict(mouth_ability_dict, orient='index')
    mouth_ability_df.index.name = "mouth_ability_id"
    mouth_ability_df.reset_index(inplace=True)

    horn_ability_df = pd.DataFrame.from_dict(horn_ability_dict, orient='index')
    horn_ability_df.index.name = "horn_ability_id"
    horn_ability_df.reset_index(inplace=True)

    back_ability_df = pd.DataFrame.from_dict(back_ability_dict, orient='index')
    back_ability_df.index.name = "back_ability_id"
    back_ability_df.reset_index(inplace=True)

    tail_ability_df = pd.DataFrame.from_dict(tail_ability_dict, orient='index')
    tail_ability_df.index.name = "tail_ability_id"
    tail_ability_df.reset_index(inplace=True)

    # Orders & Offers
    all_orders_df = pd.DataFrame(all_orders_list)
    all_orders_df["bid_or_ask"] = "ask"
    all_orders_df.rename(columns={"order_id": "order_offer_id"}, inplace=True)

    all_offers_df = pd.DataFrame(all_offers_list)
    all_offers_df["bid_or_ask"] = "bid"
    all_offers_df.rename(columns={"offer_id": "order_offer_id"}, inplace=True)

    all_bid_ask_df = pd.concat([all_orders_df, all_offers_df], ignore_index=True)
    price_cols = ["base_price", "current_price", "current_price_usd", "ended_price"]
    all_bid_ask_df[price_cols] = all_bid_ask_df[price_cols].astype(float)
    all_bid_ask_df.drop_duplicates(subset=["order_offer_id"], inplace=True)

    # Transfers
    all_transfers_df = pd.DataFrame(all_transfers_list)
    transfer_price_cols = ["with_price_usd", "with_price"]
    all_transfers_df[transfer_price_cols] = all_transfers_df[transfer_price_cols].astype(float)
    all_transfers_df.drop_duplicates(subset=["transfer_id", "axie_id"], inplace=True)

    # 6) Build a dictionary of table_name -> DataFrame for easy iteration
    table_dict = {
        "Axies": all_axies_df,
        "Eyes": eyes_parts_df,
        "Mouth": mouth_parts_df,
        "Ears": ears_parts_df,
        "Horn": horn_parts_df,
        "Back": back_parts_df,
        "Tail": tail_parts_df,
        "Mouth_Abilities": mouth_ability_df,
        "Horn_Abilities": horn_ability_df,
        "Back_Abilities": back_ability_df,
        "Tail_Abilities": tail_ability_df,
        "Order_Offers": all_bid_ask_df,
        "Transfers": all_transfers_df
    }

    # ------------------------------------------------------------------------
    # 7) For each table, upload a CSV file to the 'axie-transformed-data' container
    # ------------------------------------------------------------------------
    transformed_container = "axie-transformed-data"
    container_transformed = blob_service_client.get_container_client(transformed_container)
    try:
        container_transformed.create_container()
    except Exception:
        pass

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    uploaded_csv_files = []

    for table_name, df in table_dict.items():
        # Convert DataFrame to CSV
        csv_data = df.to_csv(index=False)

        # Create a blob name based on table_name + timestamp
        csv_blob_name = f"{table_name.lower()}_{timestamp}.csv"
        blob_client = container_transformed.get_blob_client(csv_blob_name)
        blob_client.upload_blob(csv_data, overwrite=True)

        logging.info(f"Uploaded CSV for {table_name} to {transformed_container}/{csv_blob_name}")
        uploaded_csv_files.append(csv_blob_name)

    # Return the list of uploaded CSV filenames (optional)
    return uploaded_csv_files