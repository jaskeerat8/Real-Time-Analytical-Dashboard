View Dashboard Here (Give 2 min to load) - https://aqidashboard.onrender.com/

Scrape Data from Publicaly available Government Websites for AQI measures as well as using Open Weather API to get real time updates. The Data is cleaned and appended to the AWS RDS MySQL database.
For  prediction a linear model is trained on the historical data collected from Kaggle. The regression model is trained on the date, month and the hour measure. The Label produced is shown as the output.
![Real Time System](https://github.com/jaskeerat8/Real-Time-Analytical-Dashboard/assets/32131898/d539eb8d-61be-4881-b952-18697f503269)

The Dashboard is made using plotly dash in python. The code is written in such a way to update the dashboard automatically every 1min. 
![Dashboard](https://github.com/jaskeerat8/Real-Time-Analytical-Dashboard/assets/32131898/91e1696e-f4b2-4baf-afa8-07df7280faa0)
