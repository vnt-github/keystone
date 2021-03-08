CODE_DIR=$0
STOCKS_DATA_DIR=$PWD
HTML_FORMAT=$1
TMP_DIR=$2
cd ${CODE_DIR%run.sh}/tmp/scrapy_tutorials/scrapy_tutorials
scrapy crawl profiles_$HTML_FORMAT -a html_format=$HTML_FORMAT -a tmp_dir=$TMP_DIR -a stocks_data_dir=$STOCKS_DATA_DIR --overwrite-output=$TMP_DIR/stocks_data_$HTML_FORMAT.jsonlines --loglevel=ERROR
cd ../../../
python trade.py $HTML_FORMAT $TMP_DIR