BUNDLE_DIR := bundle

clean:
	rm bundle.zip
	rm -rf $(BUNDLE_DIR)

bundle.zip: requirements.txt step/index.py
	rm -rf $(BUNDLE_DIR)
	mkdir -p $(BUNDLE_DIR)
	cp step/index.py $(BUNDLE_DIR)/
	pip install -r requirements.txt --target $(BUNDLE_DIR)
	cd $(BUNDLE_DIR) && zip -rq ../bundle.zip .
