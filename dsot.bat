@echo off
setlocal EnableDelayedExpansion

rem ==========================================
rem CONFIG
rem ==========================================
set "BASE_DIR=C:\Users\COLABORADOR QUEO.COLABORADOR.000\zenoh\dsot"

rem ==========================================
rem LOOP INTERATIVO
rem ==========================================
:menu
echo.
echo _______________________________________________________
echo.
echo DATA SENDING [O to T] - TESTE DE CONECTIVIDADE
echo _______________________________________________________
echo Comandos:
echo   open p0   ^| close p0
echo   open p1   ^| close p1
echo   open p2   ^| close p2
echo   open p3   ^| close p3
echo   open p4   ^| close p4
echo   open p5   ^| close p5
echo   open p6   ^| close p6
echo   open p7   ^| close p7
echo   open p8   ^| close p8
echo   open pT   ^| close pT
echo   exit
echo.

set /p "USERCMD=> "

if /i "%USERCMD%"=="exit" goto :eof

for /f "tokens=1,2" %%A in ("%USERCMD%") do (
    set "ACTION=%%A"
    set "PEER=%%B"
)

if not defined ACTION goto :menu
if not defined PEER goto :menu

call :resolvePeer "%PEER%"
if errorlevel 1 (
    echo Peer invalido: %PEER%
    goto :clearVars
)

if /i "%ACTION%"=="open" (
    call :openPeer
    goto :clearVars
)

if /i "%ACTION%"=="close" (
    call :closePeer
    goto :clearVars
)

echo Acao invalida: %ACTION%

:clearVars
set "ACTION="
set "PEER="
set "WINDOW_TITLE="
set "RUN_CMD="
goto :menu

rem ==========================================
rem RESOLVE PEER
rem ==========================================
:resolvePeer
set "INPUT=%~1"

if /i "%INPUT%"=="p0" (
    set "WINDOW_TITLE=PEER_p0"
    set "RUN_CMD=py ""peer_operador.py"" ""configs\peer0.json"""
    exit /b 0
)

if /i "%INPUT%"=="p1" (
    set "WINDOW_TITLE=PEER_p1"
    set "RUN_CMD=py ""peer_generic.py"" ""configs\peer1.json"""
    exit /b 0
)

if /i "%INPUT%"=="p2" (
    set "WINDOW_TITLE=PEER_p2"
    set "RUN_CMD=py ""peer_generic.py"" ""configs\peer2.json"""
    exit /b 0
)

if /i "%INPUT%"=="p3" (
    set "WINDOW_TITLE=PEER_p3"
    set "RUN_CMD=py ""peer_generic.py"" ""configs\peer3.json"""
    exit /b 0
)

if /i "%INPUT%"=="p4" (
    set "WINDOW_TITLE=PEER_p4"
    set "RUN_CMD=py ""peer_generic.py"" ""configs\peer4.json"""
    exit /b 0
)

if /i "%INPUT%"=="p5" (
    set "WINDOW_TITLE=PEER_p5"
    set "RUN_CMD=py ""peer_generic.py"" ""configs\peer5.json"""
    exit /b 0
)

if /i "%INPUT%"=="p6" (
    set "WINDOW_TITLE=PEER_p6"
    set "RUN_CMD=py ""peer_generic.py"" ""configs\peer6.json"""
    exit /b 0
)

if /i "%INPUT%"=="p7" (
    set "WINDOW_TITLE=PEER_p7"
    set "RUN_CMD=py ""peer_generic.py"" ""configs\peer7.json"""
    exit /b 0
)

if /i "%INPUT%"=="p8" (
    set "WINDOW_TITLE=PEER_p8"
    set "RUN_CMD=py ""peer_generic.py"" ""configs\peer8.json"""
    exit /b 0
)

if /i "%INPUT%"=="pT" (
    set "WINDOW_TITLE=PEER_pT"
    set "RUN_CMD=py ""peer_terminal.py"" ""configs\peerT.json"""
    exit /b 0
)

exit /b 1

rem ==========================================
rem ABRIR PEER
rem ==========================================
:openPeer
if not exist "%BASE_DIR%" (
    echo Pasta base nao encontrada: "%BASE_DIR%"
    exit /b
)

echo Abrindo %PEER%...
start "%WINDOW_TITLE%" cmd /k "cd /d ""%BASE_DIR%"" && %RUN_CMD%"
exit /b

rem ==========================================
rem FECHAR PEER
rem ==========================================
:closePeer
echo Fechando %PEER%...
taskkill /FI "WINDOWTITLE eq %WINDOW_TITLE%" /T /F >nul 2>&1

if errorlevel 1 (
    echo Nenhuma janela encontrada para %PEER%.
) else (
    echo %PEER% fechado.
)
exit /b