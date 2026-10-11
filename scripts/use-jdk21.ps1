# Use JDK 21 in this PowerShell window. Dot-source it:  . .\scripts\use-jdk21.ps1
# The PC's default JDK is 25, which breaks Mockito inline mocks in the tests.
$env:JAVA_HOME = 'C:\Program Files\Eclipse Adoptium\jdk-21.0.4.7-hotspot'
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
java -version
