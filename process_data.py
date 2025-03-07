import json
import psycopg2
import requests

def convert_registration(reg):
    return {
        "name": reg.get("name", ""),
        "email": reg.get("email", ""),
        "event": reg.get("event", ""),
        "amount": reg.get("total", ""),
        "purchasedAt": reg.get("payment_date", ""),
        "invoiceId": reg.get("invoice_number", ""),
        "orderId": reg.get("order_id", ""),
        "receiptId": reg.get("receipt_id", ""),
        "universityName": "",
        "coachName": "",
        "coachMobile": "",
        "regType": "",
        "scanned": False,
    }

def extract_registrations(output_file="output.json"):
    try:
        with open(output_file, "r") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"Error: Unable to read file '{output_file}'.")
        return []

    return [convert_registration(reg) for reg in (data if isinstance(data, list) else [data])]

def send_email(reg):
    """ Sends a single registration record via the email API """
    api_url = "https://vitopia.vitap.ac.in/api/sendemail"
    payload = {"registrations": [reg]}  # Send one record at a time
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer OXbsI3womFuXNVK9Y6FKWuq/5pwK0PM70/fK6wlmKmc="
    }

    try:
        response = requests.post(api_url, json=payload, headers=headers)
        response.raise_for_status()
        print(f"Email sent successfully for invoiceId: {reg['invoiceId']}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending email for invoiceId {reg['invoiceId']}: {e}")

def insert_data_into_postgres(output_file="output.json"):
    registrations = extract_registrations(output_file)
    if not registrations:
        print("No registrations to insert.")
        return

    conn = psycopg2.connect(
        dbname="vitopia",
        user="postgres",
        password="yeaVKbN3DINKnKU5QrHZ",
        host="database-1.cjusivpk0jjy.ap-south-1.rds.amazonaws.com",
        port="5432"
    )
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO "Registration" 
        ("name", "email", "event", "amount", "purchasedAt", "invoiceId", "orderId", "receiptId", "universityName", "coachName", "coachMobile", "regType", "scanned")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """

    for reg in registrations:
        values = (
            reg["name"], reg["email"], reg["event"], reg["amount"],
            reg["purchasedAt"], reg["invoiceId"], reg["orderId"], reg["receiptId"],
            reg["universityName"], reg["coachName"], reg["coachMobile"],
            reg["regType"], reg["scanned"]
        )

        try:
            cursor.execute(insert_query, values)
            conn.commit()
            print(f"Inserted record with invoiceId: {reg['invoiceId']}")
            send_email(reg)  # Send email immediately after inserting

        except psycopg2.Error as e:
            print(f"Error inserting record with invoiceId '{reg['invoiceId']}': {e}")
            conn.rollback()

    cursor.close()
    conn.close()
    print("Data insertion process complete.")

if __name__ == "__main__":
    insert_data_into_postgres("output.json")
