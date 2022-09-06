rem Package the library for Nuget
copy ..\MaxFactry.Provider.AzureProvider-NF-4.5.2\bin\Release\MaxFactry.Provider.Azure*.dll lib\net452\

c:\install\nuget\nuget.exe pack MaxFactry.Provider.Azure.nuspec -OutputDirectory "packages" -IncludeReferencedProjects -properties Configuration=Release 