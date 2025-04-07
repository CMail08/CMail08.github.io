import pandas as pd
from titlecase import titlecase

# Define the path to the input CSV file
input_file = r"C:\Users\colin\All_Things_Code\Projects\Springsteen_Setlists\2 - Data Sources\Sessions, Outtakes, Songs.csv"

# Read the CSV file into a DataFrame
df = pd.read_csv(input_file)

# Print out the first few rows to check the columns (optional)
print("Before conversion:")
print(df.head())

# Assuming the text to be converted is in the third column, you can access it by its index (index 2)
# If the column has a header name (e.g., 'Title'), you can replace df.iloc[:, 2] with df['Title']
df.iloc[:, 2] = df.iloc[:, 2].apply(lambda x: titlecase(str(x)))

# Check the changes (optional)
print("\nAfter conversion:")
print(df.head())

# Define the path to the output CSV file
output_file = r"C:\Users\colin\All_Things_Code\Projects\Springsteen_Setlists\2 - Data Sources\Sessions, Outtakes, Songs_proper.csv"

# Save the updated DataFrame to a new CSV file without the index column
df.to_csv(output_file, index=False)

print(f"\nFile saved successfully to: {output_file}")
