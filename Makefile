PYTHON ?= python3

Dockerfile: Dockerfile.in options.lst
	$(PYTHON) -c "import sys; sys.stdout.write(sys.stdin.read().replace('# @@', open('./options.lst', 'r').read()))" < Dockerfile.in > Dockerfile

options.lst: ./s3gc.py
	$(PYTHON) ./s3gc.py --listoptions > options.lst
