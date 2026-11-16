@echo off
setlocal
cd /d "%~dp0"
chcp 65001 >nul

where python >nul 2>nul
if errorlevel 1 (
    echo Python nao foi encontrado no PATH. Instale o Python 3 e tente novamente.
    pause
    exit /b 1
)

python -c "import websocket" >nul 2>nul
if errorlevel 1 (
    echo Instalando a dependencia websocket-client para receber os candles ao vivo...
    python -m pip install --user websocket-client
    if errorlevel 1 (
        echo Nao foi possivel instalar websocket-client.
        pause
        exit /b 1
    )
)

python "%~dp0ia_evolutiva_agressiva_btc_15m.py"
if errorlevel 1 (
    echo.
    echo A IA agressiva foi encerrada com erro. Veja a mensagem acima.
)
pause
endlocal
