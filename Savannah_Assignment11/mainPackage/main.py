#File Name : main.py
#Student Name: Ian McDaniel, Evan Bolin
#email:  mcdaniip@mail.uc.edu, bolinen@mail.uc.edu
#Assignment Number: Assignment 11
#Due Date:   April 17 2025
#Course #/Section:   IS4010-001
#Semester/Year:   Spring 2025
#Brief Description of the assignment:   Using api and python to clean up a csv file and create two new ones 
#Brief Description of what this module does.: Furthering our knowledge of the python language with api usage and csv experience 
#Citations: https://chatgpt.com

import os
from dataProcessing.cleaner import *

def main():
    input_file = os.path.join("data", "fuelPurchaseData.csv")
    cleaned_file = os.path.join("Data", "cleanedData.csv")
    anomalies_file = os.path.join("Data", "dataAnomalies.csv")

    cleaner = DataCleaner(input_file, cleaned_file, anomalies_file)
    cleaner.clean_data()
    print("Data cleaning complete. Files saved in /Data folder.")
    

if __name__ == "__main__":
    main()