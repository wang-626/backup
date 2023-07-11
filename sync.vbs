Set WshShell = CreateObject("WScript.Shell" ) 
WshShell.Run "cmd /c cd C:\py-env\py39\Scripts && activate.bat && cd C:\backup && python main.py sync" ,vbhide
Set WshShell = Nothing