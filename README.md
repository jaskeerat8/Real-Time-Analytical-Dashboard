View Dashboard Here - http://3.25.35.63:8000/ (Give a min to load).

- Scrape Data from publicly available Government Websites for AQI measures as well as use Open Weather API to get real-time updates. The Data is cleaned and appended to the AWS RDS MySQL database.
- For prediction, a linear model is trained on the historical data collected from Kaggle. The regression model is trained on the date, month and hour measure. The Label produced is shown as the output.
![Real Time System](https://github.com/user-attachments/assets/e21ff98a-f743-4aea-9e8f-801a9a13d9c2)
- The Dashboard is made using plotly dash in python. The code is written in such a way to update the dashboard automatically every 1min.

![Dashboard](https://github.com/jaskeerat8/Real-Time-Analytical-Dashboard/assets/32131898/91e1696e-f4b2-4baf-afa8-07df7280faa0)
