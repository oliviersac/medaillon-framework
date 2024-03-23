
class ArgumentParser:

    def parse_arguments(arguments):
        """
            Parse arguments and return a list

            :param arguments: Arguments that will be parsed
        """  
        # Remove the first argument (script name)
        arguments = arguments[1:]

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
