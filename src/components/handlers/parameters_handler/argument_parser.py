
class ArgumentParser:

    def parse_arguments(arguments):

        # Parse arguments
        parameters = {}
        i = 0
        while i < len(arguments):
            # Check if parameter starts with '-' and has a corresponding value
            if arguments[i].startswith('-') and i + 1 < len(arguments):
                parameters[arguments[i]] = arguments[i + 1]
                i += 1
            else:
                print("Invalid parameter:", arguments[i])
            i += 1

        return parameters