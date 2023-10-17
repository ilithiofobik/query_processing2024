##
# Practical Course High-Performance Query Processing
# Makefile
#

CC					= gcc
CXX					= g++
RM					= rm -f

INCLUDE_FLAGS	   := -Iinclude
DEBUG_FLAGS		   := -fsanitize=undefined
SANITIZE_FLAGS	   := -fno-omit-frame-pointer -fsanitize=address
RELEASE_FLAGS	   := -DNDEBUG
CXXFLAGS		   := -Wall -std=c++20 $(INCLUDE_FLAGS) -g $(CXXFLAGS) -lfmt -ltbb -pthread

BINDIR				= bin
SRCDIR				= tasks
RSRCDIR				= resources

## set target to debug if unspecified
ifeq ($(target),)
	target := debug
endif

## select flags based on target
ifeq ($(target),release)
	CXXFLAGS += $(RELEASE_FLAGS) -O3
else ifeq ($(target),staging)
	CXXFLAGS += $(DEBUG_FLAGS) -O3
else ifeq ($(target),sanitize)
	CXXFLAGS += $(SANITIZE_FLAGS) $(DEBUG_FLAGS) -O0
else
	CXXFLAGS += $(DEBUG_FLAGS) -O0
endif

## rebuild w/o file changes if target changed
TARGET_MARKER      := $(BINDIR)/.$(target).target
$(TARGET_MARKER):
	mkdir -p $(BINDIR)
	$(RM) $(BINDIR)/.*.target
	touch $(TARGET_MARKER)

## build targets
$(BINDIR)/%.out: $(TARGET_MARKER) $(SRCDIR)/%.cpp
	$(CXX) $(patsubst $(BINDIR)/%.out, $(SRCDIR)/%.cpp, $@) $(CXXFLAGS) -o $@

clean:
	$(RM) -r $(BINDIR)/*

.PHONY: clean

# end
