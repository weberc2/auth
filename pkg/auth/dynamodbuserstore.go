package auth

import (
	"encoding/base64"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/weberc2/auth/pkg/types"
)

type DynamoDBUserStore struct {
	Client *dynamodb.DynamoDB
	Table  string
}

func (ddbus *DynamoDBUserStore) Create(entry *types.UserEntry) error {
	if _, err := ddbus.Client.PutItem(&dynamodb.PutItemInput{
		TableName:           aws.String(ddbus.Table),
		Item:                userToAttributes(entry),
		ConditionExpression: aws.String("attribute_not_exists(#User)"),
		ExpressionAttributeNames: map[string]*string{
			"#User": aws.String("User"),
		},
	}); err != nil {
		if _, ok := err.(*dynamodb.ConditionalCheckFailedException); ok {
			return ErrUserExists
		}
		return fmt.Errorf("creating user: %w", err)
	}
	return nil
}

func (ddbus *DynamoDBUserStore) Upsert(entry *types.UserEntry) error {
	if _, err := ddbus.Client.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(ddbus.Table),
		Item:      userToAttributes(entry),
	}); err != nil {
		return fmt.Errorf("upserting user: %w", err)
	}
	return nil
}

func (ddbus *DynamoDBUserStore) Get(
	user types.UserID,
) (*types.UserEntry, error) {
	rsp, err := ddbus.Client.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(ddbus.Table),
		Key: map[string]*dynamodb.AttributeValue{
			"User": {S: aws.String(string(user))},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("getting user: %w", err)
	}
	if rsp.Item == nil {
		return nil, types.ErrUserNotFound
	}

	return attributesToUser(rsp.Item), nil
}

func userToAttributes(
	entry *types.UserEntry,
) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"User":  {S: aws.String(string(entry.User))},
		"Email": {S: aws.String(entry.Email)},
		"PasswordHash": {
			S: aws.String(base64.RawStdEncoding.EncodeToString(
				entry.PasswordHash,
			)),
		},
	}
}

func attributesToUser(
	attrs map[string]*dynamodb.AttributeValue,
) *types.UserEntry {
	user := types.UserID(*attrs["User"].S)
	email := *attrs["Email"].S
	data, err := base64.RawStdEncoding.DecodeString(*attrs["PasswordHash"].S)
	if err != nil {
		panic(fmt.Sprintf(
			"non-base64-encoded `PasswordHash` value for user `%s`: %v",
			user,
			err,
		))
	}
	return &types.UserEntry{User: user, Email: email, PasswordHash: data}
}
