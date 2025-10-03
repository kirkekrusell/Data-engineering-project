# Project Overview
This project aims to analyze Estonian companies’ economic activities by combining basic company data from the Business Register with economic activity areas from the MTR (Main Economic Activity Registry). The goal is to identify sector trends, regional differences, and company activity patterns across Estonia.

Stakeholders   
1. Data Analysts
2. Policymakers
3. Investors
4. Banks and Financial Institutions
5. Public and Media

Key Metrics (KPIs)
1. Number of activity notices per company
2. Share of companies with expired activity notices

Business Questions
1. How many companies have multiple activity notices and operate in multiple sectors?
2. How many companies registered their economic activity areas in the same year they were established?
3. How many companies have terminated at least one economic activity notice?
4. Which companies have a single activity notice but cover multiple economic sectors?
5. Average duration of an activity notice before expiration
6. Percentage of companies with all activity notices expired

Datasets
1. Basic Company Data (Business Register)
359,816 rows, 14 columns
Key columns: Name, Register code, Legal form, Status, First entry date, Address, Location codes

2. Notice of Economic Activities (Economic Activity Register)
35,769 rows, 8 columns
Key columns: Number, Entrepreneur name, Register code, Start & End date, Valid, Field of activity
