package auth

import "github.com/golang-jwt/jwt/v5"

// ClaimsWithSubject creates a Claims with the given subject for use in tests
// from other packages. Both RegisteredClaims.Subject and SubjectID are set.
func ClaimsWithSubject(subject string) *Claims {
	return &Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: subject},
		SubjectID:        subject,
	}
}
