rm -rfv source_0
rm -rfv source_1

sphinx-apidoc -o source_0 ../instradatore 
sphinx-apidoc -o source_1 ../handlers/ 

make clean
make html
