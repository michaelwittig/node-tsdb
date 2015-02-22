default: test

jshint:
	@echo "jshint"
	@find . -name "*.js" -not -path "./node_modules/*" -print0 | xargs -0 ./node_modules/.bin/jshint

circular:
	@echo "circular"
	@./node_modules/.bin/madge --circular --format amd .

mocha:
	@echo "mocha"
	@./node_modules/.bin/mocha -t 20000 test/*.js
	@echo

test: jshint mocha circular
	@echo "test"
	@echo

outdated:
	@echo "outdated modules?"
	@./node_modules/.bin/npmedge
