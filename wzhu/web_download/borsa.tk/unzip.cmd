# unzips all zip files into respective directories that retails the filename as the directory
# unzip -d ymd ehb{ymd}.zip

find -name '*.zip' -exec sh -c 'unzip -d "${1%.*}" "$1"' _ {} \;

#Test it first by adding an echo i.e.
#find -name '*.zip' -exec sh -c 'echo unzip -d "${1%.*}" "$1"' _ {} \;
