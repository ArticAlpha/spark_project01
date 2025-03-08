import csv
import time
from random import *
from faker import Faker
from datetime import datetime, timedelta
import uuid
import phonenumbers

# from src.testing.tester1 import generate_email

# Initialize Faker for generating fake data
fake = Faker()

start_time = time.time()
# Define the file path for the CSV
csv_file_path = "car_sales.csv"

# Define the headers for the CSV file
headers = [
    "car_id", "make", "model", "year", "color",
    "price", "discounted_price", "vin", "engine_type", "mileage", "fuel_type",

    "order_id", "order_date", "delivery_date", "showroom_name", "showroom_address", "showroom_pincode", "showroom_phone"

    , "sales_rep_name", "sales_rep_phone", "sales_rep_email", "commission_obtained", "sales_rep_department",
    "sales_rep_experience_years",

    "customer_name", "customer_age", "customer_email", "customer_phone", "customer_address", "customer_gender",
    "customer_marital_status",

    "order_amount", "order_status", "payment_method", "warranty_period"
]

# Define car combinations
car_data = [
    ["Toyota", "Camry", "Hybrid", "Petrol", 27000, 15],
    ["BMW", "X5", "V6", "Petrol", 62000, 10],
    ["Tesla", "Model S", "Electric", "Electric", 88000, 450],  # Full charge
    ["Ford", "F-150", "V6", "Diesel", 35000, 8],
    ["Hyundai", "Tucson", "Inline-4", "Petrol", 26000, 14],
    ["Audi", "Q7", "V6 Turbo", "Diesel", 58000, 9],
    ["Mercedes", "E-Class", "Inline-4 Turbo", "Petrol", 55000, 12],
    ["Honda", "CR-V", "Inline-4", "Petrol", 28000, 13],
    ["Volkswagen", "Passat", "Inline-4", "Diesel", 27000, 15],
    ["Chevrolet", "Silverado", "V8", "Diesel", 40000, 7],
    ["Nissan", "Leaf", "Electric Motor", "Electric", 30000, 380],  # Full charge
    ["Kia", "Sorento", "Inline-4 Turbo", "Petrol", 29000, 12],
    ["Jeep", "Wrangler", "V6", "Petrol", 35000, 10],
    ["Volvo", "XC90", "Hybrid", "Petrol", 57000, 11],
    ["Porsche", "Cayenne", "V6 Turbo", "Diesel", 73000, 9],
    ["Dodge", "Challenger", "V8 HEMI", "Petrol", 32000, 8],
    ["Subaru", "Outback", "Boxer-4", "Petrol", 28000, 13],
    ["Land Rover", "Discovery", "Inline-4", "Diesel", 61000, 7],
    # Additional cars
    ["Mazda", "CX-5", "Inline-4", "Petrol", 25000, 14],
    ["Chevrolet", "Equinox", "Inline-4", "Petrol", 27000, 13],
    ["Ford", "Explorer", "V6", "Petrol", 34000, 10],
    ["BMW", "3 Series", "Inline-4 Turbo", "Petrol", 41000, 12],
    ["Toyota", "RAV4", "Hybrid", "Petrol", 30000, 18],
    ["Tesla", "Model 3", "Electric Motor", "Electric", 48000, 500],  # Full charge
    ["Honda", "Accord", "Inline-4 Turbo", "Petrol", 27000, 14],
    ["Hyundai", "Santa Fe", "V6", "Petrol", 28000, 12],
    ["Nissan", "Altima", "Inline-4", "Petrol", 25000, 15],
    ["Jeep", "Grand Cherokee", "V6", "Diesel", 40000, 8],
    ["Subaru", "Forester", "Boxer-4", "Petrol", 26000, 13],
    ["Chevrolet", "Tahoe", "V8", "Petrol", 49000, 6],
    ["Toyota", "Corolla", "Inline-4", "Petrol", 20000, 16],
    ["Volkswagen", "Tiguan", "Inline-4 Turbo", "Petrol", 26000, 13],
    ["Audi", "A4", "Inline-4 Turbo", "Petrol", 39000, 12],
    ["Mercedes", "GLC", "Inline-4 Turbo", "Petrol", 45000, 10],
    ["Kia", "Sportage", "Inline-4", "Petrol", 27000, 12],
    ["Volvo", "S60", "Hybrid", "Petrol", 42000, 14],
    ["Ford", "Mustang", "V8", "Petrol", 55000, 8],
    ["Tesla", "Model Y", "Electric Motor", "Electric", 54000, 480],  # Full charge
    ["Mazda", "MX-5 Miata", "Inline-4", "Petrol", 26000, 14],
    ["Honda", "Pilot", "V6", "Petrol", 37000, 10],
    ["Chevrolet", "Blazer", "Inline-4 Turbo", "Petrol", 31000, 11],
    ["Toyota", "Highlander", "V6", "Petrol", 36000, 9],
    ["Hyundai", "Elantra", "Inline-4", "Petrol", 22000, 16],
    ["BMW", "X3", "Inline-4 Turbo", "Petrol", 44000, 11],
    ["Jeep", "Renegade", "Inline-4", "Petrol", 24000, 13],
    ["Subaru", "Crosstrek", "Boxer-4", "Petrol", 24000, 12],
    ["Volkswagen", "Jetta", "Inline-4", "Diesel", 21000, 18],
    ["Audi", "Q5", "Inline-4 Turbo", "Diesel", 43000, 11],
    ["Porsche", "Macan", "V6 Turbo", "Diesel", 67000, 9],
    ["Dodge", "Durango", "V8", "Petrol", 42000, 8],
    ["Land Rover", "Range Rover", "V6", "Diesel", 92000, 7]
]


showrooms = [
    {"showroomname": "AutoMax Motors", "showroomaddress": "123 Main Street, Downtown", "pincode": 560001,
     "phonenumber": "9876543210"},
    {"showroomname": "Speedster Autos", "showroomaddress": "45 Race Avenue, City Center", "pincode": 560002,
     "phonenumber": "9123456780"},
    {"showroomname": "Luxury Rides", "showroomaddress": "78 Elite Lane, Green Park", "pincode": 560003,
     "phonenumber": "8765432109"},
    {"showroomname": "Eco Wheels", "showroomaddress": "12 Greenway Road, Eco City", "pincode": 560004,
     "phonenumber": "9812345678"},
    {"showroomname": "Urban Drives", "showroomaddress": "99 City Plaza, Urban Square", "pincode": 560005,
     "phonenumber": "9912345678"},
    {"showroomname": "Premium Autos", "showroomaddress": "54 High Street, Elite Colony", "pincode": 560006,
     "phonenumber": "9823456712"},
    {"showroomname": "Highway Motors", "showroomaddress": "202 Highway Road, Transit Area", "pincode": 560007,
     "phonenumber": "9871234567"},
    {"showroomname": "Family Rides", "showroomaddress": "33 Suburb Lane, Family Square", "pincode": 560008,
     "phonenumber": "9123678456"},
    {"showroomname": "Turbo Automobiles", "showroomaddress": "88 Speed Road, Downtown", "pincode": 560009,
     "phonenumber": "8912345678"},
    {"showroomname": "Cityline Showroom", "showroomaddress": "15 Station Road, Cityline", "pincode": 560010,
     "phonenumber": "9012345678"},
    {"showroomname": "Elite Cars", "showroomaddress": "36 Prestige Street, Green View", "pincode": 560011,
     "phonenumber": "8812345678"},
    {"showroomname": "Zoom Autos", "showroomaddress": "77 Rapid Avenue, Zoom Park", "pincode": 560012,
     "phonenumber": "8612345678"},
    {"showroomname": "RideOn Motors", "showroomaddress": "19 Mobility Lane, RideOn Square", "pincode": 560013,
     "phonenumber": "8412345678"},
    {"showroomname": "Classic Wheels", "showroomaddress": "10 Heritage Avenue, Classic Park", "pincode": 560014,
     "phonenumber": "8212345678"},
    {"showroomname": "Metro Cars", "showroomaddress": "25 Central Plaza, Metro City", "pincode": 560015,
     "phonenumber": "8012345678"},
    {"showroomname": "SuperDrive Showroom", "showroomaddress": "70 Velocity Road, SuperDrive Area", "pincode": 560016,
     "phonenumber": "7812345678"},
    {"showroomname": "Skyline Motors", "showroomaddress": "92 Skyline Drive, Hill View", "pincode": 560017,
     "phonenumber": "7612345678"},
    {"showroomname": "Vista Automobiles", "showroomaddress": "47 Horizon Street, Vista Valley", "pincode": 560018,
     "phonenumber": "7412345678"},
    {"showroomname": "Prime Cars", "showroomaddress": "60 Prestige Avenue, Prime Estate", "pincode": 560019,
     "phonenumber": "7212345678"},
    {"showroomname": "Grand Autos", "showroomaddress": "85 Victory Lane, Grand Plaza", "pincode": 560020,
     "phonenumber": "7012345678"},
    {"showroomname": "Infinity Rides", "showroomaddress": "102 Dreamland Road, Infinity Estate", "pincode": 560021,
     "phonenumber": "6912345678"},
    {"showroomname": "AutoNation Showroom", "showroomaddress": "208 Cityline Boulevard, AutoNation Park",
     "pincode": 560022, "phonenumber": "6812345678"},
    {"showroomname": "NextGen Motors", "showroomaddress": "303 Future Lane, NextGen City", "pincode": 560023,
     "phonenumber": "6712345678"},
    {"showroomname": "Fusion Cars", "showroomaddress": "410 Harmony Road, Fusion Plaza", "pincode": 560024,
     "phonenumber": "6612345678"},
    {"showroomname": "UrbanMotion Autos",
     "showroomaddress": "520 Skyline Avenue, UrbanMotion Zone",
     "pincode": 560025,
     "phonenumber": "6512345678"
     },
    {"showroomname": "AutoHub Elite",
     "showroomaddress": "609 Prime Circle, AutoHub Park",
     "pincode": 560026,
     "phonenumber": "6412345678"
     },
    {
     "showroomname": "Velocity Showroom",
     "showroomaddress": "710 Rapid Plaza, Velocity Center",
     "pincode": 560027,
     "phonenumber": "6312345678"
     }
]


# making 500 unique sales reps
# num_sales_rep = 500


# sales_rep=[fake.name() for _ in range(num_sales_rep)]

def generate_random_date():
    """
    Generate a random date between start_date and end_date.
    :param start_date: The earliest possible date (datetime object).
    :param end_date: The latest possible date (datetime object).
    :return: A random date (datetime object).
    """
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2025, 1, 1)
    delta = end_date - start_date
    random_days = randint(0, delta.days)  # Random number of days
    random_date = start_date + timedelta(days=random_days)
    return random_date.date()


def randomize_value(value, allow_null=True):
    return choice([value, None, '']) if allow_null and randint(0, 10) < 2 else value  # 10% chance to be None or empty


# Function to generate email addresses
def generate_email(name):
    first_name, last_name = name.split()[:2]
    email = f"{first_name.lower()}.{last_name.lower()}@example.com"
    return email


def generate_customer_data(count, country_code="+1"):
    """
    Generate a dataset of unique customers with names, addresses, phone numbers, gender, and marital status.
    :param count: Number of customers to generate.
    :param country_code: Country code for the phone numbers (default: +1).
    :return: List of dictionaries containing customer details.
    """
    customer_data = []
    unique_names = set()
    unique_phone_numbers = set()
    genders = ["Male", "Female"]
    marital_statuses = ["Single", "Married"]

    while len(customer_data) < count:
        # Generate unique name
        name = fake.name().lower()
        if name in unique_names:
            continue
        unique_names.add(name)

        # Generate unique phone number
        random_number = randint(1000000000, 9999999999)
        full_number = f"{country_code}{random_number}"
        try:
            parsed_number = phonenumbers.parse(full_number, None)
            if not phonenumbers.is_valid_number(parsed_number):
                continue
            formatted_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.NATIONAL)
            if formatted_number in unique_phone_numbers:
                continue
            unique_phone_numbers.add(formatted_number)
        except phonenumbers.NumberParseException:
            continue

        # Add customer details
        customer_data.append({
            "Name": name,
            "Address": fake.address(),
            "Formatted Number": formatted_number,
            "Gender": fake.random_element(genders),
            "Marital Status": fake.random_element(marital_statuses)
        })

    return customer_data


# generating customer
customer_data = generate_customer_data(count=10000, country_code="+1")

#generating sales rep data
sales_rep_data = generate_customer_data(count=500, country_code="+1")

import random

# Shuffle the sales reps and showrooms for randomness
random.shuffle(sales_rep_data)
random.shuffle(showrooms)

# Initialize a list to store the assignments
sales_rep_assignments = []

# Assign each sales rep to a showroom
for i, sales_rep in enumerate(sales_rep_data):
    showroom = showrooms[i % len(showrooms)]  # Cycle through showrooms

    # Create an assignment entry
    sales_rep_assignments.append({
        "sales_rep_name": sales_rep["Name"],
        "sales_rep_phone": sales_rep["Formatted Number"],
        "showroom_name": showroom["showroomname"],
        "showroom_address": showroom["showroomaddress"],
        "showroom_pincode": showroom["pincode"],
        "showroom_phone":showroom["phonenumber"]
    })




initial_order_id = 10000
car_sales = []

for i in range(10000):  # Generate 100 car sales entries
    rand_value_showroom = randint(0, 26)
    rand_value_delivery_order = randint(10, 90)

    car_id = i
    car_details = choice(car_data)
    make = car_details[0]
    model = car_details[1]
    engine_type = car_details[2]
    fuel_type = car_details[3]
    manufacture_date = generate_random_date()  # user defined function to generate random date
    color = randomize_value(choice(["Red", "Blue", "Black", "White", "Silver"]))
    price = car_details[4]
    random_discount = randomize_value((randint(5, 25) / 100))
    discounted_price = price - (
                price * (random_discount if random_discount is not None and random_discount != '' else 1))
    vin = str(uuid.uuid4()) + '-' + str(car_id) + str(
        initial_order_id)  # making vin unique by adding card_id and initial_order_id
    mileage = car_details[5]  # Mileage in kilometers

    order_id = initial_order_id
    initial_order_id += 1
    order_date = manufacture_date + timedelta(days=rand_value_delivery_order)
    delivery_date = order_date + timedelta(days=rand_value_delivery_order)

    sales_rep_showroom= randint(0, 499)
    showroom_name = sales_rep_assignments[sales_rep_showroom]['showroom_name']
    showroom_address = sales_rep_assignments[sales_rep_showroom]['showroom_address']
    showroom_pincode = sales_rep_assignments[sales_rep_showroom]['showroom_pincode']
    showroom_phone = sales_rep_assignments[sales_rep_showroom]['showroom_phone']

    # sales_rep_num=randint(0, 499)
    sales_rep_name = sales_rep_assignments[sales_rep_showroom]['sales_rep_name']
    sales_rep_email = generate_email(sales_rep_name)  # user defined function to generate email
    sales_rep_phone = sales_rep_assignments[sales_rep_showroom]['sales_rep_phone']
    commission_obtained = randomize_value(round(randint(2, 10) / 100, 2))  # Random commission amount
    sales_rep_department = "Sales"
    sales_rep_experience_years = randomize_value(randint(1, 20))  # Experience in years

    customer_name = customer_data[i]["Name"]
    customer_email = generate_email(customer_name)
    customer_age = randomize_value(randint(18, 80))
    customer_phone = customer_data[i]["Formatted Number"]  # randomize_value(lis22[i])
    customer_address = customer_data[i]["Address"]  #randomize_value(fake.address())
    customer_gender = customer_data[i]["Gender"] #randomize_value(choice(["Male", "Female"]))
    customer_marital_status = customer_data[i]["Marital Status"]#randomize_value(choice(["Single", "Married"]))

    order_amount = price  # Random amount in multiples of 1000
    order_status = randomize_value(choice(["Pending", "Completed", "Cancelled", ""]))
    payment_method = randomize_value(choice(["Credit Card", "Cash", "Bank Transfer"]))
    # order_notes = randomize_value(fake.sentence(nb_words=10))
    warranty_period = randomize_value(f"{randint(1, 5)} years")

    car_sale_entry = [
        car_id, make, model, manufacture_date, color,
        price, discounted_price, vin, engine_type, mileage, fuel_type,
        order_id, order_date, delivery_date, showroom_name, showroom_address, showroom_pincode, showroom_phone
        , sales_rep_name, sales_rep_phone, sales_rep_email, commission_obtained, sales_rep_department,
        sales_rep_experience_years,
        customer_name, customer_age, customer_email, customer_phone, customer_address, customer_gender,
        customer_marital_status,
        order_amount, order_status, payment_method,
        warranty_period
    ]
    car_sales.append(car_sale_entry)

# print(car_sales)
# for i in range(100):
#     print(car_sales[i])
print(len(car_sales))

# Write the data to the CSV file
with open(csv_file_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(headers)
    writer.writerows(car_sales)

end_time = time.time()
print(f"CSV file '{csv_file_path}' has been generated successfully, time taken {end_time - start_time}.")

# print(price,discounted_price,manufacture_date,order_date,delivery_date,showroom_name,showroom_address,showroom_pincode,showroom_phone)
