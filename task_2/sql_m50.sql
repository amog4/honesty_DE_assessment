select *, avg(risk_score OVER(ORDER BY Date
     ROWS BETWEEN 50 PRECEDING AND CURRENT ROW )
     as moving_average from  data