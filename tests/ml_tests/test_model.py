import os
import pytest

@pytest.mark.skipif(os.getenv("CI") == "true", reason="Skipped in CI")
def test_training():
    return None

if __name__=="__main__":
  print(test_training())
