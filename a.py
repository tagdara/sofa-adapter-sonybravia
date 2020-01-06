import os
import subprocess

proc = subprocess.Popen(['adb', 'logcat', '-v', 'time'], stdout=subprocess.PIPE)
for line in proc.stdout:
    try:
        print(line.decode())
        if "Setting system time at" in line.decode():
            proc.kill()
            break
    except:
        print('could not decode line')
proc.wait()
