clean:
	rm -rf ./dist
build: clean
	mkdir ./dist
	cp ./src/main.py ./dist
	cd ./src && zip -x main.py -r ../dist/libs.zip .
	cd ./dist && pipenv lock -r > requirements.txt
	cd ./dist && mkdir vendor && pipenv run pip install -r requirements.txt -t ./vendor
	cd ./dist/vendor && zip -r ../vendor.zip .