HTML_FORMAT=$1
TMP_DIR=$2
cd ./tmp/scrapy_tutorials/scrapy_tutorials
scrapy crawl profiles_$HTML_FORMAT -a html_format=$HTML_FORMAT -a tmp_dir=$TMP_DIR --overwrite-output=$TMP_DIR/stocks_data_$HTML_FORMAT.jsonlines --loglevel=ERROR
cd ../../../
python trade.py $HTML_FORMAT $TMP_DIR