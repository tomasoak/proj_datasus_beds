<h1 align=center>Insights from Brazilian Public Health System based on a cloud ELT infrastructure</h1> 

<h2>Goal</h2>
Extract valuable insights from Brazilian Public Health System (SUS) from 2007 to 2023.

<br>
<br>

Thereby, this project uses Terraform as Infrastructure as Code paradigm to set up Google Cloud Platform environments, which includes IAM and Service Accounts Policies and Permissions, Cloud Composer and Airflow as orchestrator to Extract and Load the data into Cloud Storage. 
<br>
Moreover, it uses BigQuery to pull these data from Cloud Storage, optimize it structure, and uses DBT to make the needed transformations to attend the business requirements. Finally, a compelling dashboard is built using Looker to showcase the meaningful insights from these data.


<br>
<br>


<h2>Technologies</h2>

- IaC: [Terraform](https://www.terraform.io)
- Cloud: [Google Cloud Platform](https://cloud.google.com) 
- Semantic Layer: [DBT (Data build tool)](https://www.getdbt.com)
- Data Visualization: [Looker Studio](https://lookerstudio.google.com)

<br>
<br>

<img title="Architecture" alt="" src="https://github.com/tomasoak/proj_datasus_beds/blob/main/proj_architecture.png" height=411, width=685> </img>