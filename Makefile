Dockerfile: Dockerfile.in options.lst
	python3 -c "import sys; sys.stdout.write(sys.stdin.read().replace('# @@', open('./options.lst', 'r').read()))" < Dockerfile.in > Dockerfile

options.lst: ./s3gc.py
	python3 ./s3gc.py --listoptions > options.lst
