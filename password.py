import bcrypt

def hash_password(password: str) -> str:
    """Generate a bcrypt hash for the provided password."""
    # Generate a salt and hash the password
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    
    # Return the hashed password as a string
    return hashed.decode('utf-8')

def check_password(password: str, hashed_password: str) -> bool:
    """Verify a password against a stored hash."""
    # The hashpw function can also be used to check passwords
    # It will return true if the password matches the hash
    return bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8'))

if __name__ == "__main__":
    # Get password input from the user
    user_password = input("Elijah@10519: ")
    
    # Hash the password
    new_password_hash = hash_password(user_password)
    
    # Print the resulting hash
    print(f"\nThe hashed password is: {new_password_hash}")
    
    # --- Example of how to check the password ---
    print("\n--- Now let's verify it! ---")
    
    # Get another password input to check against the hash
    verify_password = input("Elijah@10519: ")
    
    # Check if the new password matches the original one
    if check_password(verify_password, new_password_hash):
        print("✅ Success! The password matches the hash.")
    else:
        print("❌ Sorry, the password does not match the hash.")