<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>CS6650 Lab 2 - Ski Resort API</title>
</head>
<body>
<h1>Ski Resort API</h1>
<p>Lab 2 Servlet is running!</p>
<p>Current time: <%= new java.util.Date() %></p>

<h2>API Endpoints:</h2>
<ul>
    <li>POST /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}</li>
    <li>GET /health</li>
    <li>WebSocket /leaderboard/{resortID}</li>
</ul>
</body>
</html>