First, thank you for considering to contributing to Kinetick!
The goal of this document is to provide everything you need to start contributing to Kinetick. 

###Assumptions

You're familiar with Github and the pull request workflow.

### Description
State what does the code do? If it's a bug fix then link to the issue. 
If it's a feature then describe how will improve the experience.

### Styleguide
- Git commit message to follow. ``<module-changed>: short descriptive message`` Ex: ``broker: add support for fyers``
- make sure the code is properly lint and follows PEP8 styleguide

### Changelog
- Update the version in ``__init__/__version__``  based on semantic version convention.
- update changelog with the version, date, your changes, username

### Test

- Make sure your code is properly tested and include Doctest 
(Yes, the tests are missing for existing code but please make sure add test for any new code from hereon.)

### Thank you again. You're awesome 🎉