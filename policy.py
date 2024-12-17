import json

class S3SessionPolicyBuilder:
    def __init__(self, version="2012-10-17"):
        # Initialize the base policy structure
        self.policy = {
            "Version": version,
            "Statement": []
        }
    
    def add_s3_statement(self, effect, actions, resources, conditions=None):
        """
        Add a statement to the session policy for S3 access.

        Parameters:
        - effect (str): Either "Allow" or "Deny".
        - actions (list or str): The S3 actions (e.g., "s3:GetObject").
        - resources (list or str): The S3 resource ARNs (e.g., "arn:aws:s3:::my-bucket/*").
        - conditions (dict, optional): Conditions to add to the statement.

        Returns:
        - self: For method chaining.
        """
        
        if isinstance(actions, str):
            actions = [actions]
        if isinstance(resources, str):
            resources = [resources]
        
        statement = {
            "Effect": effect,
            "Action": actions,
            "Resource": resources
        }
        
        if conditions is not None:
            statement["Condition"] = conditions

        self.policy["Statement"].append(statement)
        return self

    def add_kms_statement(self, effect, actions, key_arns, conditions=None):
        """
        Add a statement to the session policy for KMS key access.

        Parameters:
        - effect (str): Either "Allow" or "Deny".
        - actions (list or str): The KMS actions (e.g., ["kms:Decrypt", "kms:Encrypt"]).
        - key_arns (list or str): The ARN(s) of the KMS keys (e.g., "arn:aws:kms:us-east-1:123456789012:key/abcd-efgh").
        - conditions (dict, optional): Conditions to add to the statement.

        Returns:
        - self: For method chaining.
        """
        
        if isinstance(actions, str):
            actions = [actions]
        if isinstance(key_arns, str):
            key_arns = [key_arns]

        statement = {
            "Effect": effect,
            "Action": actions,
            "Resource": key_arns
        }

        if conditions is not None:
            statement["Condition"] = conditions

        self.policy["Statement"].append(statement)
        return self

    def build(self):
        """
        Returns the assembled policy as a Python dictionary.
        """
        return self.policy

    def to_json(self, indent=2):
        """
        Returns the assembled policy as a JSON string.
        """
        return json.dumps(self.policy, indent=indent)


# Example usage:
if __name__ == "__main__":
    # Example ARNs
    bucket_arn = "arn:aws:s3:::my-secure-bucket"
    bucket_resource_arn = "arn:aws:s3:::my-secure-bucket/*"
    kms_key_arn = "arn:aws:kms:us-east-1:123456789012:key/abcd1234-efgh-5678-ijkl-90mnopqrstuv"

    builder = S3SessionPolicyBuilder()
    
    # Add S3 read/write permissions for the bucket
    builder.add_s3_statement(
        effect="Allow",
        actions=["s3:GetObject", "s3:PutObject"],
        resources=bucket_resource_arn
    )
    
    # Add KMS permissions necessary for decrypting/encrypting S3 objects 
    # encrypted with the specified KMS key
    builder.add_kms_statement(
        effect="Allow",
        actions=["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"],
        key_arns=kms_key_arn
    )

    # Convert the resulting policy to JSON
    session_policy_json = builder.to_json()
    print(session_policy_json)

    # This can now be used as the `Policy` parameter in an sts:AssumeRole call.
    # Example:
    # sts_client.assume_role(
    #     RoleArn="arn:aws:iam::123456789012:role/YourRoleName",
    #     RoleSessionName="ExampleSession",
    #     Policy=session_policy_json
    # )