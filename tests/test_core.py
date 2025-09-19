"""Test cases for tap-twitter-api."""

import unittest
from unittest.mock import patch, MagicMock, Mock
import json
from tap_twitterapi.tap import TapTwitter
from tap_twitterapi.streams import UserInfoStream, UserTweetsStream, HashtagTweetsStream


class TestTapTwitter(unittest.TestCase):
    """Test cases for TapTwitter."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "api_key": "test_key",
            "usernames": ["testuser"],
            "hashtags": ["testhashtag"],
            "start_date": "2025-01-01T00:00:00Z",
            "max_records_per_entity": 10,
            "enable_child_streams": False,  # Disable for simpler testing
        }

    def test_tap_initialization(self):
        """Test that the tap initializes correctly."""
        tap = TapTwitter(config=self.config)
        self.assertIsNotNone(tap)
        self.assertEqual(tap.name, "tap-twitter-api")

    def test_discover_streams(self):
        """Test stream discovery."""
        tap = TapTwitter(config=self.config)
        streams = tap.discover_streams()
        
        # Should have 4 base streams (users, user_tweets, mentions, hashtag_tweets)
        self.assertEqual(len(streams), 4)
        
        stream_names = {stream.name for stream in streams}
        expected_names = {"users", "user_tweets", "mentions", "hashtag_tweets"}
        self.assertEqual(stream_names, expected_names)

    def test_discover_streams_with_children(self):
        """Test stream discovery with child streams enabled."""
        config_with_children = self.config.copy()
        config_with_children["enable_child_streams"] = True
        
        tap = TapTwitter(config=config_with_children)
        streams = tap.discover_streams()
        
        # Should have 6 streams (4 base + 2 child)
        self.assertEqual(len(streams), 6)
        
        stream_names = {stream.name for stream in streams}
        expected_names = {
            "users", "user_tweets", "mentions", "hashtag_tweets",
            "tweet_replies", "tweet_quotes"
        }
        self.assertEqual(stream_names, expected_names)

    @patch.object(UserInfoStream, '_request')
    def test_user_info_stream(self, mock_request):
        """Test UserInfoStream."""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "id": "12345",
                "userName": "testuser",
                "name": "Test User",
                "url": "https://twitter.com/testuser",
                "createdAt": "2020-01-01T00:00:00Z",
                "followers": 100,
                "following": 50,
                "isBlueVerified": True,
            }
        }
        mock_response.headers = {}
        mock_request.return_value = mock_response

        # Create tap and get user info stream
        tap = TapTwitter(config=self.config)
        user_stream = next(s for s in tap.discover_streams() if s.name == "users")
        
        # Test stream properties
        self.assertEqual(user_stream.name, "users")
        self.assertEqual(user_stream.primary_keys, ["id"])
        self.assertIsNone(user_stream.replication_key)
        
        # Get records
        records = list(user_stream.get_records(None))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], "12345")
        self.assertEqual(records[0]["_tap_username"], "testuser")

    @patch.object(UserTweetsStream, '_request')
    def test_user_tweets_stream(self, mock_request):
        """Test UserTweetsStream."""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "tweets": [
                {
                    "id": "tweet123",
                    "text": "Test tweet",
                    "createdAt": "2025-01-15T00:00:00Z",
                    "likeCount": 10,
                    "retweetCount": 5,
                    "replyCount": 2,
                    "quoteCount": 1,
                    "viewCount": 100,
                    "author": {
                        "id": "12345",
                        "userName": "testuser",
                        "name": "Test User"
                    }
                }
            ],
            "has_next_page": False
        }
        mock_response.headers = {}
        mock_request.return_value = mock_response

        # Create tap and get tweets stream
        tap = TapTwitter(config=self.config)
        tweets_stream = next(s for s in tap.discover_streams() if s.name == "user_tweets")
        
        # Test stream properties
        self.assertEqual(tweets_stream.name, "user_tweets")
        self.assertEqual(tweets_stream.primary_keys, ["id"])
        self.assertEqual(tweets_stream.replication_key, "createdAt")
        
        # Get records
        records = list(tweets_stream.get_records(None))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], "tweet123")
        self.assertEqual(records[0]["_tap_username"], "testuser")

    @patch('tap_twitterapi.streams.RESTStream._request')
    def test_mentions_stream(self, mock_request):
        """Test MentionsStream."""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "tweets": [
                {
                    "id": "mention123",
                    "text": "@testuser mentioned",
                    "createdAt": "2025-01-15T00:00:00Z",
                    "author": {
                        "id": "67890",
                        "userName": "otheruser",
                        "name": "Other User"
                    }
                }
            ],
            "has_next_page": False
        }
        mock_response.headers = {}
        mock_request.return_value = mock_response

        # Create tap and get mentions stream
        tap = TapTwitter(config=self.config)
        mentions_stream = next(s for s in tap.discover_streams() if s.name == "mentions")
        
        # Test stream properties
        self.assertEqual(mentions_stream.name, "mentions")
        self.assertEqual(mentions_stream.primary_keys, ["id"])
        self.assertEqual(mentions_stream.replication_key, "createdAt")
        
        # Get records
        records = list(mentions_stream.get_records(None))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["_tap_username"], "testuser")

    @patch.object(HashtagTweetsStream, '_request')
    def test_hashtag_tweets_stream(self, mock_request):
        """Test HashtagTweetsStream."""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "tweets": [
                {
                    "id": "hashtag123",
                    "text": "Tweet with #testhashtag",
                    "createdAt": "2025-01-15T00:00:00Z",
                    "entities": {
                        "hashtags": [
                            {"text": "testhashtag"}
                        ]
                    },
                    "author": {
                        "id": "11111",
                        "userName": "hashuser",
                        "name": "Hash User"
                    }
                }
            ],
            "has_next_page": False
        }
        mock_response.headers = {}
        mock_request.return_value = mock_response

        # Create tap and get hashtag stream
        tap = TapTwitter(config=self.config)
        hashtag_stream = next(s for s in tap.discover_streams() if s.name == "hashtag_tweets")
        
        # Test stream properties
        self.assertEqual(hashtag_stream.name, "hashtag_tweets")
        self.assertEqual(hashtag_stream.primary_keys, ["id"])
        self.assertEqual(hashtag_stream.replication_key, "createdAt")
        
        # Get records
        records = list(hashtag_stream.get_records(None))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["_tap_hashtag"], "testhashtag")

    def test_config_validation(self):
        """Test configuration validation."""
        # Test missing required field
        invalid_config = {"usernames": ["test"]}
        
        # Creating tap with invalid config should work
        tap = TapTwitter(config=invalid_config)
        self.assertIsNotNone(tap)
        
        # But accessing streams that need api_key should fail
        with self.assertRaises(KeyError):
            streams = tap.discover_streams()
            user_stream = next(s for s in streams if s.name == "users")
            list(user_stream.get_records(None))

    def test_authenticator(self):
        """Test authenticator creation."""
        tap = TapTwitter(config=self.config)
        stream = next(s for s in tap.discover_streams() if s.name == "users")
        
        authenticator = stream.authenticator
        self.assertIsNotNone(authenticator)
        
        headers = authenticator.auth_headers
        self.assertIn("X-API-Key", headers)
        self.assertEqual(headers["X-API-Key"], "test_key")

    def test_stream_url_params(self):
        """Test URL parameter generation for streams."""
        tap = TapTwitter(config=self.config)
        
        # Test UserInfoStream
        user_stream = next(s for s in tap.discover_streams() if s.name == "users")
        params = user_stream.get_url_params({"username": "testuser"}, None)
        self.assertEqual(params["userName"], "testuser")
        
        # Test UserTweetsStream
        tweets_stream = next(s for s in tap.discover_streams() if s.name == "user_tweets")
        params = tweets_stream.get_url_params({"username": "testuser"}, None)
        self.assertIn("query", params)
        self.assertIn("from:testuser", params["query"])
        self.assertEqual(params["queryType"], "Latest")
        
        # Test with pagination cursor
        params = tweets_stream.get_url_params({"username": "testuser"}, "next_cursor_123")
        self.assertEqual(params["cursor"], "next_cursor_123")

    def test_record_limits(self):
        """Test that record limits are properly applied."""
        config_with_limits = self.config.copy()
        config_with_limits["max_records_per_entity"] = 2
        
        tap = TapTwitter(config=config_with_limits)
        
        # Mock a stream that would return many records
        with patch.object(UserTweetsStream, '_request') as mock_request:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "tweets": [
                    {"id": f"tweet{i}", "text": f"Tweet {i}", "createdAt": "2025-01-15T00:00:00Z"}
                    for i in range(10)
                ],
                "has_next_page": False
            }
            mock_response.headers = {}
            mock_request.return_value = mock_response
            
            tweets_stream = next(s for s in tap.discover_streams() if s.name == "user_tweets")
            records = list(tweets_stream.get_records(None))
            
            # Should only return 2 records due to limit
            self.assertEqual(len(records), 2)


if __name__ == "__main__":
    unittest.main()
    