@echo off
title Rappi Dashboard — Actualizando...
echo.
echo ============================================
echo   Rappi Search Dashboard — Actualizando
echo ============================================
echo.
echo Se abrira el navegador para login con OKTA.
echo Inicia sesion y vuelve a esta ventana.
echo.
cd /d "%~dp0"
"C:\Users\david.gil\AppData\Local\Python\pythoncore-3.14-64\python.exe" refresh.py
echo.
pause
