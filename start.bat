@echo off
echo ====================================================
echo   World Cup 2026 Predictor
echo ====================================================
echo.
echo   App:   http://localhost:8080
echo   Admin: http://localhost:8080/admin
echo   Admin password: admin1234
echo.
echo   To use MongoDB Atlas (persistent storage):
echo   Set MONGO_URI=mongodb+srv://... below
echo   Otherwise data is saved in the local data\ folder
echo.

set ADMIN_PASSWORD=admin1234

REM Paste your MongoDB Atlas connection string here (optional):
REM set MONGO_URI=mongodb+srv://user:password@cluster.mongodb.net/wc2026

"C:\Program Files\Anaconda3\python.exe" "%~dp0server.py"
pause
