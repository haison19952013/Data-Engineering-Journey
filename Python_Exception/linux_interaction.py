def linux_interaction():
    import sys
    if "alinux" not in sys.platform:
        raise RuntimeError("Function can only run on Linux systems.")
    print("Doing Linux things.")

# ...

# ...

try:
    linux_interaction()
finally:
    print("Cleaning up, irrespective of any exceptions.")