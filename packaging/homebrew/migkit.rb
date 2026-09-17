class Migkit < Formula
  include Language::Python::Virtualenv

  desc "Verify, repair, and move databases across engines"
  homepage "https://github.com/arkashira/migkit"
  license "MIT"
  head "https://github.com/arkashira/migkit.git", branch: "master"

  # Filled in by `brew create --python https://files.pythonhosted.org/...`
  # once migkit is on PyPI. Until then `brew install --HEAD` works.
  url "https://files.pythonhosted.org/packages/source/m/migkit/migkit-0.2.0.tar.gz"
  sha256 "0" * 64

  depends_on "libpq"
  depends_on "python@3.12"

  # Programs migkit drives for capabilities beyond verification. Each is
  # optional at runtime - `migkit doctor` reports what is present - but a
  # Homebrew install may as well be complete.
  depends_on "mysql-client" => :recommended
  depends_on "mongodb/brew/mongodb-database-tools" => :optional
  depends_on "mydumper" => :optional
  depends_on "pgloader" => :optional
  depends_on "percona-toolkit" => :optional

  # Python dependencies are generated with:
  #   brew update-python-resources Formula/migkit.rb
  # They are deliberately not hand-written: migkit requires every engine
  # driver, so the list is long and must match pyproject.toml exactly.

  def install
    virtualenv_install_with_resources
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/migkit --version")
    # doctor must work with no configuration at all
    assert_match "capability", shell_output("#{bin}/migkit doctor")
  end
end
