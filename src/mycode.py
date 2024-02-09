import git

# Specify the GitHub repository URL
github_repo_url = 'https://git@github.com/michTalebzadeh/rhes76_DSBQ.git'

# Specify the local directory where you want to fetch the code
local_directory = '/work/hduser'

# Clone the GitHub repository (if it doesn't exist locally) or fetch updates (if it exists)
repo = git.Repo.init(local_directory)
print(repo)
if not repo.remotes:
    repo.create_remote('origin', github_repo_url)
origin = repo.remote('origin')
print(origin)
origin.fetch()


