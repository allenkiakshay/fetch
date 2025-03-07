import json
import psycopg2

def convert_registration(reg):
    """
    Converts a single registration record from the original JSON format to the desired schema.
    
    Mapping:
      - name, email, amount, purchasedAt, invoiceId, orderId, receiptId are taken directly.
      - regType: extracted from product_meta as the first part before " - ".
      - event: extracted from product_meta as the text after "Ticket:".
      - universityName, coachName, coachMobile: extracted from field_values; if the value is "None", it is replaced with an empty string.
      - scanned: defaults to False.
    """
    # Extract basic fields from the original data
    name = reg.get("name", "")
    email = reg.get("email", "")
    total = reg.get("total", "")
    payment_date = reg.get("payment_date", "")
    invoice_number = reg.get("invoice_number", "")
    order_id = reg.get("order_id", "")
    receipt_id = reg.get("receipt_id", "")
    product_meta = reg.get("product_meta", "")

    # Parse regType from product_meta: first part before " - "
    regType = ""
    if product_meta:
        parts = product_meta.split(" - ")
        if parts:
            regType = parts[0].strip()

    # Parse event from product_meta: text after "Ticket:" if it exists
    event = ""
    if "Ticket:" in product_meta:
        event = product_meta.split("Ticket:")[-1].strip()

    # Default values for university and coach details
    universityName = ""
    coachName = ""
    coachMobile = ""

    # Search within field_values for university and coach/manager details.
    field_values = reg.get("field_values", [])
    for field in field_values:
        # Skip if the field is None
        if field is None:
            continue

        # Safely retrieve field_name and field_value.
        field_name = (field.get("field_name") or "").strip().lower()
        field_value = (field.get("field_value") or "").strip()

        # Replace "None" (as a string) with an empty string.
        if field_value.lower() == "none":
            field_value = ""

        if "university" in field_name:
            universityName = field_value
        if "name" in field_name and ("coach" in field_name or "manager" in field_name):
            coachName = field_value
        if "mobile" in field_name and ("coach" in field_name or "manager" in field_name):
            coachMobile = field_value

    # Build the new registration dictionary according to the schema.
    new_reg = {
        "name": name,
        "email": email,
        "event": event,
        "amount": total,
        "purchasedAt": payment_date,  # Ideally, convert to a proper DateTime if needed.
        "invoiceId": invoice_number,
        "orderId": order_id,
        "receiptId": receipt_id,
        "universityName": universityName,
        "coachName": coachName,
        "coachMobile": coachMobile,
        "regType": regType,
        "scanned": False,
    }
    return new_reg

def extract_registrations(output_file="output.json"):
    """
    Reads the original registration data from the specified JSON file,
    converts each record to the desired format, and returns a list of records.
    """
    try:
        with open(output_file, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File '{output_file}' not found.")
        return []
    except json.JSONDecodeError:
        print(f"Error: File '{output_file}' does not contain valid JSON.")
        return []

    # If the JSON data is a single record, wrap it in a list for uniform processing.
    registrations = data if isinstance(data, list) else [data]
    converted_regs = [convert_registration(reg) for reg in registrations]
    return converted_regs

def insert_data_into_postgres(output_file="output.json"):
    """
    Reads the registration data from the output file, converts each record,
    and inserts the data into the existing PostgreSQL table, skipping duplicates.
    """
    registrations = extract_registrations(output_file)
    if not registrations:
        print("No registrations to insert.")
        return

    # Update these connection parameters with your PostgreSQL details.
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
            reg.get("name", ""),
            reg.get("email", ""),
            reg.get("event", ""),
            reg.get("amount", ""),
            reg.get("purchasedAt", ""),
            reg.get("invoiceId", ""),
            reg.get("orderId", ""),
            reg.get("receiptId", ""),
            reg.get("universityName", ""),
            reg.get("coachName", ""),
            reg.get("coachMobile", ""),
            reg.get("regType", ""),
            reg.get("scanned", False)  # PostgreSQL can store Boolean types.
        )
        try:
            cursor.execute(insert_query, values)
        except psycopg2.Error as e:
            print(f"Error while inserting record with invoiceId '{reg.get('invoiceId', '')}': {e}")
            conn.rollback()
        else:
            conn.commit()

    cursor.close()
    conn.close()
    print("Data insertion complete.")

if __name__ == "__main__":
    insert_data_into_postgres("output.json")
