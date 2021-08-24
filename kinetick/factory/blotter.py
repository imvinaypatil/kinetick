from kinetick.blotter import Blotter


class MainBlotter(Blotter):
    pass  # we just need the name


def build():
    blotter = MainBlotter()
    return blotter


# ===========================================
if __name__ == "__main__":
    build().run()
